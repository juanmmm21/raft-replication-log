"""Replicación de entradas de log: comprobación de consistencia de
AppendEntries, fusión/truncado del sufijo divergente, y avance de
`commit_index` por quorum -- incluida la regla de seguridad de §5.4.2
(un líder solo comprometé directamente entradas de su propio término).
"""

from __future__ import annotations

import pytest

from raft_replication_log import pipeline, replication
from raft_replication_log.errors import NotLeaderError
from raft_replication_log.models import AppendEntriesRequest, LogEntry, RaftRole

from .conftest import make_cluster, make_node


class _NullTransport:
    def send_request_vote(self, sender: object, target: object, request: object) -> None:
        return None

    def send_append_entries(self, sender: object, target: object, request: object) -> None:
        return None

    def send_install_snapshot(self, sender: object, target: object, request: object) -> None:
        return None


def test_propose_rejected_when_not_leader() -> None:
    node = make_node("n0", ["n1", "n2"], _NullTransport(), seed=1)
    assert node.role == RaftRole.FOLLOWER
    with pytest.raises(NotLeaderError):
        node.propose(b"cmd")


def test_propose_appends_to_leader_log() -> None:
    node = make_node("n0", ["n1", "n2"], _NullTransport(), seed=1)
    node.role = RaftRole.LEADER
    node.current_term = 1
    index = replication.propose(node, b"cmd-1")
    assert index == 1
    assert node.log == [LogEntry(index=1, term=1, command=b"cmd-1")]


def test_handle_append_entries_heartbeat_resets_role_and_leader() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.role = RaftRole.CANDIDATE
    node.current_term = 1
    request = AppendEntriesRequest(
        term=1, leader_id="n1", prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0
    )
    response = replication.handle_append_entries(node, request)
    assert response.success is True
    assert node.role == RaftRole.FOLLOWER
    assert node.leader_id == "n1"


def test_handle_append_entries_rejects_stale_term() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 5
    request = AppendEntriesRequest(
        term=3, leader_id="n1", prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0
    )
    response = replication.handle_append_entries(node, request)
    assert response.success is False
    assert response.term == 5


def test_handle_append_entries_rejects_on_missing_prev_entry() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 1
    request = AppendEntriesRequest(
        term=1, leader_id="n1", prev_log_index=5, prev_log_term=1, entries=(), leader_commit=0
    )
    response = replication.handle_append_entries(node, request)
    assert response.success is False
    assert response.conflict_index == 1  # log vacío: el líder debe retroceder hasta el principio


def test_handle_append_entries_rejects_on_term_mismatch_at_prev() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 2
    node.log = [LogEntry(index=1, term=1, command=b"a")]
    request = AppendEntriesRequest(
        term=2, leader_id="n1", prev_log_index=1, prev_log_term=2, entries=(), leader_commit=0
    )
    response = replication.handle_append_entries(node, request)
    assert response.success is False
    assert response.conflict_term == 1
    assert response.conflict_index == 1


def test_handle_append_entries_truncates_conflicting_suffix() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 2
    node.log = [
        LogEntry(index=1, term=1, command=b"a"),
        LogEntry(index=2, term=1, command=b"stale"),
        LogEntry(index=3, term=1, command=b"also-stale"),
    ]
    request = AppendEntriesRequest(
        term=2,
        leader_id="n1",
        prev_log_index=1,
        prev_log_term=1,
        entries=(LogEntry(index=2, term=2, command=b"fresh"),),
        leader_commit=0,
    )
    response = replication.handle_append_entries(node, request)
    assert response.success is True
    assert [e.index for e in node.log] == [1, 2]
    assert node.log[-1].command == b"fresh"


def test_handle_append_entries_does_not_truncate_on_duplicate_resend() -> None:
    """Reenviar exactamente las mismas entradas (p.ej. un heartbeat que
    repite el último AppendEntries por una respuesta perdida) no debe
    truncar nada por delante del punto de divergencia real -- si lo
    hiciera, se perdería temporalmente una entrada ya comprometida."""
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 1
    node.log = [
        LogEntry(index=1, term=1, command=b"a"),
        LogEntry(index=2, term=1, command=b"b"),
    ]
    request = AppendEntriesRequest(
        term=1,
        leader_id="n1",
        prev_log_index=0,
        prev_log_term=0,
        entries=(
            LogEntry(index=1, term=1, command=b"a"),
            LogEntry(index=2, term=1, command=b"b"),
        ),
        leader_commit=0,
    )
    response = replication.handle_append_entries(node, request)
    assert response.success is True
    assert [e.command for e in node.log] == [b"a", b"b"]


def test_handle_append_entries_applies_committed_entries() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 1
    request = AppendEntriesRequest(
        term=1,
        leader_id="n1",
        prev_log_index=0,
        prev_log_term=0,
        entries=(LogEntry(index=1, term=1, command=b"x"),),
        leader_commit=1,
    )
    replication.handle_append_entries(node, request)
    assert node.commit_index == 1
    assert node.state_machine.applied_commands == (b"x",)  # type: ignore[attr-defined]


def test_leader_only_commits_current_term_entries_directly() -> None:
    """Regla de seguridad de §5.4.2: aunque una entrada de un término
    anterior ya esté replicada en mayoría, el líder no debe adelantar
    `commit_index` hasta ella por conteo directo -- solo cuando una
    entrada de su propio término alcanza quorum, arrastrando consigo
    (indirectamente) todo lo anterior."""
    node = make_node("n0", ["n1", "n2"], _NullTransport(), seed=1)
    node.role = RaftRole.LEADER
    node.current_term = 2
    node.log = [LogEntry(index=1, term=1, command=b"old")]  # de un término anterior
    node.next_index = {"n1": 2, "n2": 2}
    node.match_index = {"n1": 0, "n2": 0}

    from raft_replication_log.models import AppendEntriesResponse

    replication.handle_append_entries_response(
        node, "n1", AppendEntriesResponse(term=2, success=True, responder_id="n1", match_index=1)
    )
    assert node.commit_index == 0, "no debe comprometerse una entrada de un término anterior sola"

    node.propose(b"new-term-entry")  # índice 2, término actual (2)
    replication.handle_append_entries_response(
        node, "n1", AppendEntriesResponse(term=2, success=True, responder_id="n1", match_index=2)
    )
    assert node.commit_index == 2, "al comprometer índice 2 (término actual), arrastra también el 1"


def test_end_to_end_quorum_replication_via_cluster() -> None:
    cluster = make_cluster(size=5, seed=2)
    leader_id = pipeline.elect_leader(cluster)
    index = pipeline.replicate_command(cluster, b"SET x=1")
    assert index == 1
    leader = cluster.nodes[leader_id]
    assert leader.commit_index >= 1
    assert leader.state_machine.applied_commands == (b"SET x=1",)  # type: ignore[attr-defined]
