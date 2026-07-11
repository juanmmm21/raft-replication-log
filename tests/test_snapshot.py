"""Snapshotting: compactación de log, RPC `InstallSnapshot` para poner al
día a un seguidor demasiado atrasado, y disparo automático por
`snapshot_threshold`.
"""

from __future__ import annotations

import pytest

from raft_replication_log import pipeline, snapshot
from raft_replication_log.errors import SnapshotOutOfRangeError
from raft_replication_log.models import InstallSnapshotRequest, LogEntry, RaftRole, Snapshot

from .conftest import make_cluster, make_node


class _NullTransport:
    def send_request_vote(self, sender: object, target: object, request: object) -> None:
        return None

    def send_append_entries(self, sender: object, target: object, request: object) -> None:
        return None

    def send_install_snapshot(self, sender: object, target: object, request: object) -> None:
        return None


def _node_with_applied_log(n: int) -> object:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.role = RaftRole.LEADER
    node.current_term = 1
    for i in range(1, n + 1):
        node.log.append(LogEntry(index=i, term=1, command=f"cmd-{i}".encode()))
    node.commit_index = n
    node.last_applied = 0
    node.apply_committed()
    return node


def test_create_snapshot_rejects_index_beyond_last_applied() -> None:
    node = _node_with_applied_log(3)
    with pytest.raises(SnapshotOutOfRangeError):
        snapshot.create_snapshot(node, up_to_index=10)


def test_create_snapshot_rejects_index_below_one() -> None:
    node = _node_with_applied_log(3)
    with pytest.raises(SnapshotOutOfRangeError):
        snapshot.create_snapshot(node, up_to_index=0)


def test_create_snapshot_truncates_log_and_preserves_state() -> None:
    node = _node_with_applied_log(5)
    result = snapshot.create_snapshot(node, up_to_index=3)
    assert result.last_included_index == 3
    assert result.last_included_term == 1
    assert [e.index for e in node.log] == [4, 5]
    assert node.snapshot is result
    assert node.last_log_index() == 5
    assert node.term_at(3) == 1  # todavía resoluble vía snapshot, aunque ya no esté en `log`


def test_create_snapshot_is_noop_if_already_covered() -> None:
    node = _node_with_applied_log(5)
    first = snapshot.create_snapshot(node, up_to_index=4)
    second = snapshot.create_snapshot(node, up_to_index=2)
    assert second is first
    assert node.snapshot is first


def test_build_install_snapshot_request_requires_existing_snapshot() -> None:
    node = _node_with_applied_log(3)
    with pytest.raises(SnapshotOutOfRangeError):
        snapshot.build_install_snapshot_request(node)


def test_handle_install_snapshot_restores_state_and_discards_covered_log() -> None:
    follower = make_node("n1", ["n0"], _NullTransport(), seed=2)
    incoming = Snapshot(last_included_index=5, last_included_term=2, state=b"\x00\x00\x00\x01x")
    request = InstallSnapshotRequest(term=2, leader_id="n0", snapshot=incoming)

    response = snapshot.handle_install_snapshot(follower, request)

    assert response.term == 2
    assert follower.snapshot == incoming
    assert follower.commit_index == 5
    assert follower.last_applied == 5
    assert follower.log == []
    assert follower.role == RaftRole.FOLLOWER
    assert follower.leader_id == "n0"
    assert follower.state_machine.applied_commands == (b"x",)  # type: ignore[attr-defined]


def test_handle_install_snapshot_keeps_valid_suffix_when_entry_matches() -> None:
    follower = make_node("n1", ["n0"], _NullTransport(), seed=2)
    follower.current_term = 2
    follower.log = [
        LogEntry(index=3, term=2, command=b"c3"),
        LogEntry(index=4, term=2, command=b"c4"),
        LogEntry(index=5, term=2, command=b"c5"),
    ]
    incoming = Snapshot(last_included_index=4, last_included_term=2, state=b"")
    request = InstallSnapshotRequest(term=2, leader_id="n0", snapshot=incoming)

    snapshot.handle_install_snapshot(follower, request)

    assert [e.index for e in follower.log] == [5]  # sufijo posterior conservado


def test_handle_install_snapshot_wipes_log_on_divergence() -> None:
    follower = make_node("n1", ["n0"], _NullTransport(), seed=2)
    follower.current_term = 2
    follower.log = [LogEntry(index=4, term=1, command=b"stale")]  # término distinto al del snapshot
    incoming = Snapshot(last_included_index=4, last_included_term=2, state=b"")
    request = InstallSnapshotRequest(term=2, leader_id="n0", snapshot=incoming)

    snapshot.handle_install_snapshot(follower, request)

    assert follower.log == []


def test_handle_install_snapshot_ignores_stale_snapshot() -> None:
    follower = make_node("n1", ["n0"], _NullTransport(), seed=2)
    follower.current_term = 2
    follower.snapshot = Snapshot(last_included_index=10, last_included_term=2, state=b"newer")
    stale = Snapshot(last_included_index=4, last_included_term=1, state=b"older")
    request = InstallSnapshotRequest(term=2, leader_id="n0", snapshot=stale)

    snapshot.handle_install_snapshot(follower, request)

    assert follower.snapshot.last_included_index == 10  # no se pisa con uno más viejo


def test_handle_install_snapshot_rejects_stale_term() -> None:
    follower = make_node("n1", ["n0"], _NullTransport(), seed=2)
    follower.current_term = 5
    incoming = Snapshot(last_included_index=1, last_included_term=1, state=b"")
    request = InstallSnapshotRequest(term=2, leader_id="n0", snapshot=incoming)

    response = snapshot.handle_install_snapshot(follower, request)

    assert response.term == 5
    assert follower.snapshot is None


def test_automatic_snapshot_triggered_by_threshold() -> None:
    cluster = make_cluster(size=3, seed=4, snapshot_threshold=4)
    pipeline.elect_leader(cluster)
    for i in range(10):
        pipeline.replicate_command(cluster, f"cmd-{i}".encode())

    for node in cluster.nodes.values():
        assert node.snapshot is not None
        assert len(node.log) < 10  # el log se compactó, no crece indefinidamente


def test_lagging_node_catches_up_via_install_snapshot_after_recovery() -> None:
    cluster = make_cluster(size=3, seed=5, snapshot_threshold=3)
    leader_id = pipeline.elect_leader(cluster)
    lagging = next(n for n in cluster.nodes if n != leader_id)

    cluster.crash_node(lagging)
    for i in range(8):
        pipeline.replicate_command(cluster, f"cmd-{i}".encode())

    # el líder ya compactó por encima del punto en que se quedó `lagging`
    assert cluster.nodes[leader_id].snapshot is not None

    cluster.recover_node(lagging)
    cluster.tick(60)

    leader_applied = cluster.nodes[leader_id].state_machine.applied_commands  # type: ignore[attr-defined]
    lagging_applied = cluster.nodes[lagging].state_machine.applied_commands  # type: ignore[attr-defined]
    assert lagging_applied == leader_applied
