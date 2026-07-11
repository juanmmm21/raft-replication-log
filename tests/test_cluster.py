"""Cluster simulado de extremo a extremo: los escenarios explícitos del
DoD de `AGENTS.md` -- elección de líder, replicación normal, caída y
recuperación de un nodo, partición de red sin split-brain, y que una
minoría aislada nunca comprometa entradas por su cuenta.
"""

from __future__ import annotations

import random

import pytest

from raft_replication_log import pipeline
from raft_replication_log.cluster import Cluster
from raft_replication_log.errors import RaftError
from raft_replication_log.models import RaftRole

from .conftest import make_cluster


def test_leader_election_in_five_node_cluster() -> None:
    cluster = make_cluster(size=5, seed=10)
    leader_id = pipeline.elect_leader(cluster)
    assert cluster.nodes[leader_id].role == RaftRole.LEADER
    other_roles = {n.role for nid, n in cluster.nodes.items() if nid != leader_id}
    assert other_roles == {RaftRole.FOLLOWER}


def test_leader_election_in_three_node_cluster() -> None:
    cluster = make_cluster(size=3, seed=11)
    leader_id = pipeline.elect_leader(cluster)
    assert cluster.nodes[leader_id].role == RaftRole.LEADER


@pytest.mark.parametrize("bad_size", [1, 2, 6, 10])
def test_cluster_construction_rejects_out_of_range_sizes(bad_size: int) -> None:
    with pytest.raises(ValueError):
        Cluster([f"n{i}" for i in range(bad_size)], rng=random.Random(0))


def test_normal_replication_reaches_all_nodes() -> None:
    cluster = make_cluster(size=5, seed=12)
    leader_id = pipeline.elect_leader(cluster)
    for payload in (b"SET a=1", b"SET b=2", b"SET c=3"):
        pipeline.replicate_command(cluster, payload)

    cluster.tick(
        cluster.nodes[leader_id].heartbeat_interval_ticks + 2
    )  # deja converger a los followers

    expected = (b"SET a=1", b"SET b=2", b"SET c=3")
    for node in cluster.nodes.values():
        assert node.state_machine.applied_commands == expected  # type: ignore[attr-defined]


def test_recovery_after_follower_crash() -> None:
    cluster = make_cluster(size=5, seed=13)
    leader_id = pipeline.elect_leader(cluster)
    follower_id = next(n for n in cluster.nodes if n != leader_id)

    cluster.crash_node(follower_id)
    for i in range(5):
        pipeline.replicate_command(cluster, f"cmd-{i}".encode())

    cluster.recover_node(follower_id)
    cluster.tick(50)

    leader_applied = cluster.nodes[leader_id].state_machine.applied_commands  # type: ignore[attr-defined]
    follower_applied = cluster.nodes[follower_id].state_machine.applied_commands  # type: ignore[attr-defined]
    assert follower_applied == leader_applied
    assert len(leader_applied) == 5


def test_recovery_after_leader_crash_elects_new_leader_and_keeps_committed_entries() -> None:
    cluster = make_cluster(size=5, seed=14)
    leader_id = pipeline.elect_leader(cluster)
    pipeline.replicate_command(cluster, b"SET a=1")
    old_term = cluster.nodes[leader_id].current_term

    cluster.crash_node(leader_id)
    new_leader_id = pipeline.elect_leader(cluster)
    assert new_leader_id != leader_id
    assert cluster.nodes[new_leader_id].current_term > old_term

    index = pipeline.replicate_command(cluster, b"SET b=2")
    assert index == 2
    # la entrada comprometida antes de la caída sobrevive (leader completeness)
    assert cluster.nodes[new_leader_id].entry_at(1).command == b"SET a=1"  # type: ignore[union-attr]

    cluster.recover_node(leader_id)
    cluster.tick(60)
    old_leader_applied = cluster.nodes[leader_id].state_machine.applied_commands  # type: ignore[attr-defined]
    assert old_leader_applied == (b"SET a=1", b"SET b=2")


def test_network_partition_never_produces_two_leaders_in_same_term() -> None:
    cluster = make_cluster(size=5, seed=15)
    leader_id = pipeline.elect_leader(cluster)
    node_ids = list(cluster.nodes)
    minority = {leader_id, next(n for n in node_ids if n != leader_id)}
    majority = set(node_ids) - minority

    cluster.partition(minority, majority)
    cluster.tick(80)

    for term, leaders in cluster.leaders_by_term().items():
        assert len(leaders) <= 1, (
            f"split-brain: {leaders} son líderes simultáneos en el término {term}"
        )


def test_isolated_minority_cannot_commit_entries_on_its_own() -> None:
    cluster = make_cluster(size=5, seed=16)
    node_ids = list(cluster.nodes)
    leader_id = pipeline.elect_leader(cluster)

    minority = {leader_id, next(n for n in node_ids if n != leader_id)}
    majority = set(node_ids) - minority
    cluster.partition(minority, majority)
    cluster.tick(80)

    old_leader = cluster.nodes[leader_id]
    commit_before = old_leader.commit_index
    try:
        old_leader.propose(b"deberia quedarse sin comprometer")
    except RaftError:
        pass
    cluster.tick(40)

    assert old_leader.commit_index == commit_before, (
        "una minoría (incluso con el antiguo líder) nunca debe poder "
        "comprometer una entrada nueva sin quorum de mayoría"
    )


def test_majority_partition_keeps_making_progress_during_partition() -> None:
    cluster = make_cluster(size=5, seed=17)
    node_ids = list(cluster.nodes)
    leader_id = pipeline.elect_leader(cluster)

    minority = {leader_id, next(n for n in node_ids if n != leader_id)}
    majority = set(node_ids) - minority
    cluster.partition(minority, majority)
    cluster.tick(80)

    new_leader_id = cluster.current_leader()
    assert new_leader_id in majority
    index = pipeline.replicate_command(cluster, b"progreso durante la particion")
    assert index >= 1


def test_partition_heals_and_minority_node_catches_up() -> None:
    cluster = make_cluster(size=5, seed=18)
    node_ids = list(cluster.nodes)
    leader_id = pipeline.elect_leader(cluster)

    minority = {leader_id, next(n for n in node_ids if n != leader_id)}
    majority = set(node_ids) - minority
    cluster.partition(minority, majority)
    cluster.tick(80)
    pipeline.replicate_command(cluster, b"SET during partition")

    cluster.heal_partition()
    cluster.tick(80)

    new_leader_id = pipeline.elect_leader(cluster)
    final_index = pipeline.replicate_command(cluster, b"SET after heal")
    cluster.tick(cluster.nodes[new_leader_id].heartbeat_interval_ticks + 2)

    reference = cluster.nodes[new_leader_id].state_machine.applied_commands  # type: ignore[attr-defined]
    for node in cluster.nodes.values():
        assert node.state_machine.applied_commands == reference  # type: ignore[attr-defined]
    assert final_index >= 2
