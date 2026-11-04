"""Estado persistente de Raft en disco (§5.1: `currentTerm`, `votedFor`,
`log[]`, más el snapshot de §7).

Diseño: el núcleo del protocolo (`node.py`, `election.py`,
`replication.py`, `snapshot.py`) sigue mutando su estado en memoria sin
saber nada de disco — así la simulación en proceso no cambia en absoluto.
`DiskStorage.sync(node)` compara ese estado con lo último persistido y
escribe sólo la diferencia, con `fsync`, y el runtime de red
(`network.py`) la llama al final de CADA operación sobre el nodo y ANTES
de enviar cualquier mensaje que esa operación haya producido. Es la regla
de Raft: el estado persistente llega a almacenamiento estable antes de
responder a un RPC (o de pedir votos, o de contar el propio log para un
quorum).

Ficheros del directorio:

* ``meta.json`` — `{"term", "voted_for"}`, escrito de forma atómica
  (temporal + fsync + rename + fsync del directorio).
* ``log.bin`` — registros append-only
  ``[u32 longitud][u32 crc32][u64 índice][u64 término][comando]``. Al
  cargar se lee hasta el primer registro truncado o con CRC inválido (una
  escritura a medias del último append antes de un crash) y el fichero se
  recorta ahí: esa entrada nunca fue confirmada a nadie, porque el
  confirmar ocurre después del fsync.
* ``snapshot.bin`` — ``[u64 índice][u64 término][u32 crc32][estado]``,
  atómico. Tras instalar o crear un snapshot el log se reescribe (atómico)
  con las entradas posteriores.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

from .errors import RaftError
from .models import LogEntry, NodeId, Snapshot, Term

if TYPE_CHECKING:
    from .node import RaftNode

_RECORD_HEADER = struct.Struct("!IIQQ")  # longitud (índice+término+comando), crc, índice, término
_SNAPSHOT_HEADER = struct.Struct("!QQI")


class StorageCorruptionError(RaftError):
    """Un fichero persistente es ilegible más allá de lo que explica un
    crash (p.ej. un snapshot con CRC inválido, o índices no consecutivos)."""


@dataclass(slots=True)
class PersistentState:
    term: Term = 0
    voted_for: NodeId | None = None
    entries: list[LogEntry] = field(default_factory=list)
    snapshot: Snapshot | None = None


def _fsync_directory(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _encode_record(entry: LogEntry) -> bytes:
    body = struct.pack("!QQ", entry.index, entry.term) + entry.command
    crc = zlib.crc32(body)
    return struct.pack("!II", len(body), crc) + body


class DiskStorage:
    def __init__(self, directory: str | Path, *, fsync: bool = True) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._fsync = fsync
        self._meta_path = self.directory / "meta.json"
        self._log_path = self.directory / "log.bin"
        self._snapshot_path = self.directory / "snapshot.bin"
        self._meta: tuple[Term, NodeId | None] = (0, None)
        self._snapshot_key: tuple[int, int] | None = None
        # (índice, término, offset en log.bin) de cada entrada persistida
        self._stored: list[tuple[int, int, int]] = []
        self._log_size = 0
        self._log_file: BinaryIO | None = None  # abierto en load()

    # ------------------------------------------------------------------
    # Carga
    # ------------------------------------------------------------------

    def load(self) -> PersistentState:
        state = PersistentState()
        if self._meta_path.exists():
            raw = json.loads(self._meta_path.read_text(encoding="utf-8"))
            state.term = int(raw["term"])
            state.voted_for = raw.get("voted_for")
        if self._snapshot_path.exists():
            state.snapshot = self._read_snapshot()
        state.entries = self._read_log(state.snapshot)
        self._meta = (state.term, state.voted_for)
        self._snapshot_key = (
            (state.snapshot.last_included_index, state.snapshot.last_included_term)
            if state.snapshot is not None
            else None
        )
        self._log_file = open(self._log_path, "ab")  # noqa: SIM115 — vive con el storage
        return state

    def _read_snapshot(self) -> Snapshot:
        data = self._snapshot_path.read_bytes()
        if len(data) < _SNAPSHOT_HEADER.size:
            raise StorageCorruptionError("snapshot.bin truncado")
        index, term, crc = _SNAPSHOT_HEADER.unpack_from(data)
        state = data[_SNAPSHOT_HEADER.size :]
        if zlib.crc32(state) != crc:
            raise StorageCorruptionError("snapshot.bin con CRC inválido")
        return Snapshot(last_included_index=index, last_included_term=term, state=state)

    def _read_log(self, snapshot: Snapshot | None) -> list[LogEntry]:
        entries: list[LogEntry] = []
        self._stored = []
        if not self._log_path.exists():
            self._log_path.touch()
            self._log_size = 0
            return entries
        data = self._log_path.read_bytes()
        offset = 0
        valid_end = 0
        while offset + 8 <= len(data):
            length, crc = struct.unpack_from("!II", data, offset)
            body_start = offset + 8
            body_end = body_start + length
            if length < 16 or body_end > len(data):
                break  # cola a medias de un append interrumpido
            body = data[body_start:body_end]
            if zlib.crc32(body) != crc:
                break
            index, term = struct.unpack_from("!QQ", body)
            expected = (
                entries[-1].index + 1
                if entries
                else (snapshot.last_included_index + 1 if snapshot is not None else index)
            )
            if index != expected:
                raise StorageCorruptionError(
                    f"log.bin: índice {index} donde se esperaba {expected}"
                )
            entries.append(LogEntry(index=index, term=term, command=body[16:]))
            self._stored.append((index, term, offset))
            offset = body_end
            valid_end = offset
        if valid_end != len(data):
            with open(self._log_path, "r+b") as handle:
                handle.truncate(valid_end)
                handle.flush()
                if self._fsync:
                    os.fsync(handle.fileno())
        self._log_size = valid_end
        return entries

    # ------------------------------------------------------------------
    # Sincronización con el estado en memoria del nodo
    # ------------------------------------------------------------------

    def sync(self, node: RaftNode) -> bool:
        """Persiste lo que haya cambiado desde la última llamada. Devuelve
        si escribió algo."""
        changed = False
        snapshot = node.snapshot
        snapshot_key = (
            (snapshot.last_included_index, snapshot.last_included_term)
            if snapshot is not None
            else None
        )
        if snapshot is not None and snapshot_key != self._snapshot_key:
            self._write_snapshot(snapshot)
            self._rewrite_log(node.log)
            self._snapshot_key = snapshot_key
            changed = True
        else:
            changed = self._sync_log(node.log) or changed
        meta = (node.current_term, node.voted_for)
        if meta != self._meta:
            self._write_meta(meta)
            self._meta = meta
            changed = True
        return changed

    def _write_atomic(self, path: Path, payload: bytes) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "wb") as handle:
            handle.write(payload)
            handle.flush()
            if self._fsync:
                os.fsync(handle.fileno())
        os.replace(tmp, path)
        if self._fsync:
            _fsync_directory(self.directory)

    def _write_meta(self, meta: tuple[Term, NodeId | None]) -> None:
        payload = json.dumps({"term": meta[0], "voted_for": meta[1]}).encode("utf-8")
        self._write_atomic(self._meta_path, payload)

    def _write_snapshot(self, snapshot: Snapshot) -> None:
        header = _SNAPSHOT_HEADER.pack(
            snapshot.last_included_index, snapshot.last_included_term, zlib.crc32(snapshot.state)
        )
        self._write_atomic(self._snapshot_path, header + snapshot.state)

    def _rewrite_log(self, entries: list[LogEntry]) -> None:
        assert self._log_file is not None, "load() debe llamarse antes de sync()"
        self._log_file.close()
        payload = bytearray()
        stored: list[tuple[int, int, int]] = []
        for entry in entries:
            stored.append((entry.index, entry.term, len(payload)))
            payload += _encode_record(entry)
        self._write_atomic(self._log_path, bytes(payload))
        self._stored = stored
        self._log_size = len(payload)
        self._log_file = open(self._log_path, "ab")  # noqa: SIM115

    def _sync_log(self, entries: list[LogEntry]) -> bool:
        assert self._log_file is not None, "load() debe llamarse antes de sync()"
        stored = self._stored
        if not entries and not stored:
            return False
        # Último índice común con el mismo término (Raft sólo trunca sufijos
        # y añade al final, así que basta retroceder desde el final).
        node_first = entries[0].index if entries else None
        stored_first = stored[0][0] if stored else None
        common = -1  # posición en `stored` de la última entrada coincidente
        if entries and stored and node_first == stored_first:
            limit = min(len(entries), len(stored))
            position = limit - 1
            while position >= 0 and stored[position][1] != entries[position].term:
                position -= 1
            common = position
        elif entries and stored:
            # Primeras entradas distintas sin snapshot nuevo: sólo ocurre si
            # el log en memoria se reemplazó; se reescribe entero.
            self._rewrite_log(entries)
            return True
        if common == len(stored) - 1 and len(entries) == len(stored):
            return False
        handle = self._log_file
        if common < len(stored) - 1:
            cut = stored[common + 1][2]
            handle.flush()
            os.truncate(self._log_path, cut)
            del stored[common + 1 :]
            self._log_size = cut
        payload = bytearray()
        for entry in entries[common + 1 :]:
            stored.append((entry.index, entry.term, self._log_size + len(payload)))
            payload += _encode_record(entry)
        handle.write(payload)
        handle.flush()
        if self._fsync:
            os.fsync(handle.fileno())
        self._log_size += len(payload)
        return True

    def close(self) -> None:
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None


def restore_node(node: RaftNode, state: PersistentState) -> None:
    """Inicializa un `RaftNode` recién construido con el estado persistido:
    término, voto, log y snapshot (que se restaura en la máquina de estados;
    `commit_index`/`last_applied` arrancan en su último índice, porque todo
    lo incluido en un snapshot estaba comprometido)."""
    node.current_term = state.term
    node.voted_for = state.voted_for
    node.snapshot = state.snapshot
    node.log = list(state.entries)
    if state.snapshot is not None:
        node.state_machine.restore_snapshot(state.snapshot.state)
        node.commit_index = state.snapshot.last_included_index
        node.last_applied = state.snapshot.last_included_index
