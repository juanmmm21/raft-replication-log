"""Fixtures compartidas: construir clusters/nodos con `random.Random`
seedeado explícito -- nunca aleatoriedad sin semilla (regla B de
`CLAUDE.md`), crítica aquí porque el propio DoD pide timeouts de
elección aleatorizados como mecanismo central del protocolo.
"""

from __future__ import annotations

import random
from collections.abc import Iterable

import pytest

from raft_replication_log.cluster import Cluster
from raft_replication_log.models import NodeId
from raft_replication_log.node import RaftNode


def make_node(
    node_id: NodeId,
    peer_ids: Iterable[NodeId],
    transport: object,
    seed: int = 0,
    election_timeout_ticks: tuple[int, int] = (10, 20),
    heartbeat_interval_ticks: int = 3,
    snapshot_threshold: int | None = None,
) -> RaftNode:
    return RaftNode(
        node_id=node_id,
        peer_ids=peer_ids,
        transport=transport,  # type: ignore[arg-type]
        rng=random.Random(seed),
        election_timeout_ticks=election_timeout_ticks,
        heartbeat_interval_ticks=heartbeat_interval_ticks,
        snapshot_threshold=snapshot_threshold,
    )


def make_cluster(
    size: int = 5,
    seed: int = 0,
    election_timeout_ticks: tuple[int, int] = (10, 20),
    heartbeat_interval_ticks: int = 3,
    snapshot_threshold: int | None = None,
) -> Cluster:
    node_ids = [f"node-{i}" for i in range(size)]
    return Cluster(
        node_ids,
        rng=random.Random(seed),
        election_timeout_ticks=election_timeout_ticks,
        heartbeat_interval_ticks=heartbeat_interval_ticks,
        snapshot_threshold=snapshot_threshold,
    )


@pytest.fixture
def cluster5() -> Cluster:
    return make_cluster(size=5, seed=1)


@pytest.fixture
def cluster3() -> Cluster:
    return make_cluster(size=3, seed=1)
