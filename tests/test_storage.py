"""Persistencia en disco: término/voto, log append-only con truncado de
sufijo, cola rota tras un crash, snapshot con compactación, y reconstrucción
de un nodo a partir de lo persistido."""

from __future__ import annotations

import random
from pathlib import Path

import pytest

from raft_replication_log.models import LogEntry, Snapshot
from raft_replication_log.node import RaftNode
from raft_replication_log.state_machine import InMemoryStateMachine
from raft_replication_log.storage import DiskStorage, StorageCorruptionError, restore_node


class _NullTransport:
    def send_request_vote(self, *args: object) -> None:
        return None

    def send_append_entries(self, *args: object) -> None:
        return None

    def send_install_snapshot(self, *args: object) -> None:
        return None


def _node(machine: InMemoryStateMachine | None = None) -> RaftNode:
    return RaftNode("n1", ["n2", "n3"], _NullTransport(), random.Random(0), state_machine=machine)


def _reload(path: Path) -> DiskStorage:
    storage = DiskStorage(path)
    return storage


def test_term_vote_and_log_survive_reopen(tmp_path: Path) -> None:
    storage = DiskStorage(tmp_path)
    storage.load()
    node = _node()
    node.current_term, node.voted_for = 3, "n2"
    node.log = [LogEntry(1, 1, b"a"), LogEntry(2, 3, b"b")]
    assert storage.sync(node) is True
    assert storage.sync(node) is False  # nada nuevo que escribir
    storage.close()

    state = DiskStorage(tmp_path).load()
    assert (state.term, state.voted_for) == (3, "n2")
    assert state.entries == node.log


def test_suffix_truncation_and_append(tmp_path: Path) -> None:
    storage = DiskStorage(tmp_path)
    storage.load()
    node = _node()
    node.log = [LogEntry(i, 1, bytes([i])) for i in range(1, 6)]
    storage.sync(node)
    # un líder nuevo sobrescribe el sufijo desde el índice 3
    node.log = node.log[:2] + [LogEntry(3, 2, b"x"), LogEntry(4, 2, b"y")]
    storage.sync(node)
    node.log = []  # conflicto en la primera entrada: todo fuera
    storage.sync(node)
    node.log = [LogEntry(1, 4, b"z")]
    storage.sync(node)
    storage.close()
    assert DiskStorage(tmp_path).load().entries == [LogEntry(1, 4, b"z")]


def test_torn_tail_is_discarded_on_load(tmp_path: Path) -> None:
    storage = DiskStorage(tmp_path)
    storage.load()
    node = _node()
    node.log = [LogEntry(1, 1, b"commit"), LogEntry(2, 1, b"medio-escrito")]
    storage.sync(node)
    storage.close()
    log_path = tmp_path / "log.bin"
    data = log_path.read_bytes()
    log_path.write_bytes(data[:-5])  # crash a mitad del último append
    reopened = DiskStorage(tmp_path)
    assert reopened.load().entries == [LogEntry(1, 1, b"commit")]
    reopened.close()
    # el recorte es definitivo y los appends posteriores siguen bien
    reopened = DiskStorage(tmp_path)
    reopened.load()
    node.log = [LogEntry(1, 1, b"commit"), LogEntry(2, 2, b"nuevo")]
    reopened.sync(node)
    reopened.close()
    assert DiskStorage(tmp_path).load().entries == node.log


def test_flipped_bit_in_last_record_is_treated_as_torn(tmp_path: Path) -> None:
    storage = DiskStorage(tmp_path)
    storage.load()
    node = _node()
    node.log = [LogEntry(1, 1, b"aaaa"), LogEntry(2, 1, b"bbbb")]
    storage.sync(node)
    storage.close()
    log_path = tmp_path / "log.bin"
    data = bytearray(log_path.read_bytes())
    data[-1] ^= 0x01
    log_path.write_bytes(bytes(data))
    assert DiskStorage(tmp_path).load().entries == [LogEntry(1, 1, b"aaaa")]


def test_snapshot_compacts_log_and_restores_state_machine(tmp_path: Path) -> None:
    machine = InMemoryStateMachine()
    storage = DiskStorage(tmp_path)
    storage.load()
    node = _node(machine)
    node.current_term = 2
    node.log = [LogEntry(i, 2, f"cmd{i}".encode()) for i in range(1, 6)]
    node.commit_index = 5
    node.apply_committed()
    node.create_snapshot(3)
    storage.sync(node)
    storage.close()

    state = DiskStorage(tmp_path).load()
    assert state.snapshot is not None and state.snapshot.last_included_index == 3
    assert [e.index for e in state.entries] == [4, 5]
    fresh_machine = InMemoryStateMachine()
    fresh = _node(fresh_machine)
    restore_node(fresh, state)
    assert (fresh.commit_index, fresh.last_applied, fresh.last_log_index()) == (3, 3, 5)
    assert fresh_machine.take_snapshot() == state.snapshot.state


def test_corrupt_snapshot_is_an_error_not_silence(tmp_path: Path) -> None:
    storage = DiskStorage(tmp_path)
    storage.load()
    node = _node()
    node.snapshot = Snapshot(1, 1, b"estado")
    storage.sync(node)
    storage.close()
    path = tmp_path / "snapshot.bin"
    data = bytearray(path.read_bytes())
    data[-1] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(StorageCorruptionError):
        DiskStorage(tmp_path).load()
