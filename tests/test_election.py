"""Elección de líder: timeouts aleatorizados, términos monotónicos y las
reglas de "election safety"/"leader completeness" al conceder votos.
"""

from __future__ import annotations

import random

from raft_replication_log import election
from raft_replication_log.cluster import Cluster
from raft_replication_log.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    NodeId,
    RaftRole,
    RequestVoteRequest,
    RequestVoteResponse,
)

from .conftest import make_cluster, make_node


class _NullTransport:
    """Transporte que descarta todo -- para tests que aíslan un único
    `RaftNode` y disparan RPCs a mano, sin que el propio nodo intente
    contactar peers reales que no existen en el test."""

    def send_request_vote(
        self, sender: NodeId, target: NodeId, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        return None

    def send_append_entries(
        self, sender: NodeId, target: NodeId, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        return None

    def send_install_snapshot(
        self, sender: NodeId, target: NodeId, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse | None:
        return None


def test_random_election_timeout_within_range() -> None:
    rng = random.Random(0)
    for _ in range(200):
        timeout = election.random_election_timeout(rng, (10, 20))
        assert 10 <= timeout <= 20


def test_random_election_timeout_deterministic_with_same_seed() -> None:
    first = [election.random_election_timeout(random.Random(7), (5, 30)) for _ in range(20)]
    second = [election.random_election_timeout(random.Random(7), (5, 30)) for _ in range(20)]
    assert first == second


def test_single_stable_leader_emerges(cluster5: Cluster) -> None:
    for _ in range(100):
        cluster5.tick()
        if cluster5.current_leader() is not None:
            break
    leaders = [n for n in cluster5.nodes.values() if n.role == RaftRole.LEADER]
    assert len(leaders) == 1
    followers = [n for n in cluster5.nodes.values() if n.role == RaftRole.FOLLOWER]
    assert len(followers) == 4


def test_election_safety_at_most_one_leader_per_term(cluster5: Cluster) -> None:
    for _ in range(150):
        cluster5.tick()
        for term, leaders in cluster5.leaders_by_term().items():
            assert len(leaders) <= 1, f"dos líderes en el término {term}: {leaders}"


def test_term_is_monotonically_increasing_across_repeated_elections() -> None:
    cluster = make_cluster(
        size=5, seed=3, election_timeout_ticks=(6, 10), heartbeat_interval_ticks=3
    )
    seen_terms: list[int] = []
    for _ in range(400):
        cluster.tick()
        leader_id = cluster.current_leader()
        if leader_id is not None:
            term = cluster.nodes[leader_id].current_term
            if not seen_terms or seen_terms[-1] != term:
                seen_terms.append(term)
            cluster.crash_node(leader_id)  # fuerza una nueva elección
            if len(seen_terms) >= 4:
                break
    assert len(seen_terms) >= 3
    assert seen_terms == sorted(seen_terms)
    assert len(set(seen_terms)) == len(seen_terms)  # estrictamente creciente, nunca repetido


def test_candidate_becomes_leader_only_after_quorum_of_votes() -> None:
    node = make_node("n0", ["n1", "n2", "n3", "n4"], _NullTransport(), seed=1)
    election.start_election(node)
    assert node.role == RaftRole.CANDIDATE  # ningún voto real llegó (transporte nulo)

    node.register_vote("n1")  # 2/5: todavía no es mayoría
    assert node.role == RaftRole.CANDIDATE

    node.register_vote("n2")  # 3/5: mayoría de 5 nodos
    # register_vote no dispara become_leader por sí solo; el chequeo de
    # quorum vive en handle_request_vote_response -- se simula aquí.
    if node.vote_count() >= node.quorum_size():
        node.become_leader()
    assert node.role == RaftRole.LEADER


def test_handle_request_vote_grants_when_log_at_least_as_up_to_date() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    request = RequestVoteRequest(term=1, candidate_id="n1", last_log_index=0, last_log_term=0)
    response = election.handle_request_vote(node, request)
    assert response.vote_granted is True
    assert node.voted_for == "n1"


def test_handle_request_vote_rejects_stale_term() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 5
    request = RequestVoteRequest(term=3, candidate_id="n1", last_log_index=0, last_log_term=0)
    response = election.handle_request_vote(node, request)
    assert response.vote_granted is False
    assert response.term == 5


def test_handle_request_vote_only_votes_once_per_term() -> None:
    node = make_node("n0", ["n1", "n2"], _NullTransport(), seed=1)
    first = election.handle_request_vote(
        node, RequestVoteRequest(term=1, candidate_id="n1", last_log_index=0, last_log_term=0)
    )
    second = election.handle_request_vote(
        node, RequestVoteRequest(term=1, candidate_id="n2", last_log_index=0, last_log_term=0)
    )
    assert first.vote_granted is True
    assert second.vote_granted is False
    assert node.voted_for == "n1"


def test_handle_request_vote_rejects_less_up_to_date_log() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    node.current_term = 3
    node.log = [LogEntry(index=1, term=3, command=b"x")]
    request = RequestVoteRequest(term=3, candidate_id="n1", last_log_index=0, last_log_term=0)
    response = election.handle_request_vote(node, request)
    assert response.vote_granted is False


def test_step_down_on_seeing_higher_term_in_vote_response() -> None:
    node = make_node("n0", ["n1"], _NullTransport(), seed=1)
    election.start_election(node)
    assert node.role == RaftRole.CANDIDATE
    candidate_term = node.current_term
    higher_term = candidate_term + 5
    election.handle_request_vote_response(
        node, RequestVoteResponse(term=higher_term, vote_granted=False, voter_id="n1")
    )
    assert node.role == RaftRole.FOLLOWER
    assert node.current_term == higher_term
    assert node.voted_for is None
