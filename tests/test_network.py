"""Raft sobre sockets TCP reales (hilos en este proceso, un puerto por
nodo, log persistente en un directorio por nodo): elección, replicación,
caída del líder, reinicio desde disco y clusters de 5 nodos."""

from __future__ import annotations

import socket
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from raft_replication_log.errors import NotLeaderError
from raft_replication_log.network import NetworkNode
from raft_replication_log.state_machine import InMemoryStateMachine

DEADLINE = 15.0


def _free_ports(count: int) -> list[int]:
    sockets = []
    for _ in range(count):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    ports = [s.getsockname()[1] for s in sockets]
    for sock in sockets:
        sock.close()
    return ports


class LocalCluster:
    def __init__(self, root: Path, size: int) -> None:
        self.root = root
        self.ids = [f"n{i}" for i in range(1, size + 1)]
        ports = _free_ports(size)
        self.addresses = {n: ("127.0.0.1", p) for n, p in zip(self.ids, ports, strict=True)}
        self.nodes: dict[str, NetworkNode] = {}
        self.machines: dict[str, InMemoryStateMachine] = {}
        for node_id in self.ids:
            self.start(node_id)

    def start(self, node_id: str) -> NetworkNode:
        machine = InMemoryStateMachine()
        peers = {p: a for p, a in self.addresses.items() if p != node_id}
        node = NetworkNode(
            node_id,
            self.addresses[node_id],
            peers,
            data_dir=str(self.root / node_id),
            state_machine=machine,
            seed=hash(node_id) & 0xFFFF,
        ).start()
        self.nodes[node_id] = node
        self.machines[node_id] = machine
        return node

    def stop(self, node_id: str) -> None:
        self.nodes.pop(node_id).stop()

    def leader(self, timeout: float = DEADLINE) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            leaders = [n for n, node in self.nodes.items() if node.is_leader]
            if len(leaders) == 1:
                return leaders[0]
            time.sleep(0.02)
        raise AssertionError(f"sin líder único tras {timeout}s")

    def replicate(self, command: bytes) -> int:
        deadline = time.monotonic() + DEADLINE
        while time.monotonic() < deadline:
            leader = self.nodes[self.leader()]
            try:
                index, _ = leader.submit(command)
            except NotLeaderError:
                continue
            if leader.wait_for(lambda n, i=index: n.commit_index >= i, timeout=5.0):
                return index
        raise AssertionError("el comando no se comprometió a tiempo")

    def converged(self, count: int, timeout: float = DEADLINE) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = {n: m.applied_commands for n, m in self.machines.items() if n in self.nodes}
            if all(len(s) == count for s in states.values()) and len(set(states.values())) == 1:
                return True
            time.sleep(0.02)
        return False

    def close(self) -> None:
        for node_id in list(self.nodes):
            self.stop(node_id)


@pytest.fixture()
def cluster3(tmp_path: Path) -> Iterator[LocalCluster]:
    cluster = LocalCluster(tmp_path, 3)
    yield cluster
    cluster.close()


def test_elects_one_leader_and_replicates(cluster3: LocalCluster) -> None:
    for i in range(20):
        cluster3.replicate(f"cmd-{i}".encode())
    assert cluster3.converged(20)
    commands = cluster3.machines[cluster3.ids[0]].applied_commands
    assert commands == tuple(f"cmd-{i}".encode() for i in range(20))


def test_follower_rejects_proposals_with_leader_hint(cluster3: LocalCluster) -> None:
    leader = cluster3.leader()
    cluster3.replicate(b"warm-up")  # todos conocen ya al líder
    follower = next(n for n in cluster3.ids if n != leader)
    with pytest.raises(NotLeaderError) as info:
        cluster3.nodes[follower].submit(b"x")
    assert info.value.known_leader == leader


def test_leader_crash_failover_and_restart_from_disk(cluster3: LocalCluster) -> None:
    for i in range(5):
        cluster3.replicate(f"a{i}".encode())
    old_leader = cluster3.leader()
    cluster3.stop(old_leader)
    new_leader = cluster3.leader()
    assert new_leader != old_leader
    for i in range(5):
        cluster3.replicate(f"b{i}".encode())
    # el antiguo líder vuelve con su término, voto y log del disco y se pone al día
    restarted = cluster3.start(old_leader)
    assert restarted.node.last_log_index() >= 5
    assert cluster3.converged(10)
    assert cluster3.machines[old_leader].applied_commands[5:] == tuple(
        f"b{i}".encode() for i in range(5)
    )


def test_whole_cluster_restart_keeps_committed_log(tmp_path: Path) -> None:
    cluster = LocalCluster(tmp_path, 3)
    try:
        for i in range(8):
            cluster.replicate(f"c{i}".encode())
        term_before = cluster.nodes[cluster.leader()].status()["term"]
    finally:
        cluster.close()
    cluster = LocalCluster(tmp_path, 3)
    try:
        cluster.replicate(b"after-restart")
        assert cluster.converged(9)
        status = cluster.nodes[cluster.leader()].status()
        assert isinstance(status["term"], int) and isinstance(term_before, int)
        assert status["term"] > term_before
    finally:
        cluster.close()


def test_five_node_cluster_tolerates_two_failures(tmp_path: Path) -> None:
    cluster = LocalCluster(tmp_path, 5)
    try:
        cluster.replicate(b"one")
        leader = cluster.leader()
        cluster.stop(leader)
        cluster.stop(next(n for n in cluster.ids if n in cluster.nodes))
        cluster.replicate(b"two")
        assert cluster.converged(2)
    finally:
        cluster.close()


def test_rejects_unsupported_cluster_sizes(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        NetworkNode("n1", ("127.0.0.1", 0), {"n2": ("127.0.0.1", 1)}, data_dir=str(tmp_path))
