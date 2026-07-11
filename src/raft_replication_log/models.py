"""Tipos de datos del protocolo de consenso Raft.

Este módulo NO importa `write-ahead-log-recovery` ni ningún otro repo
hermano (regla del ecosistema: la integración real solo ocurre dentro de
`nanosql`). `LogEntry` redefine localmente, con el nombre y la forma que
Raft necesita, el concepto de "entrada de log que se replica": a
diferencia del `LogRecord` de `write-ahead-log-recovery` (registro ARIES
completo con tipo de operación, tabla, clave, imágenes antes/después),
Raft solo necesita saber el índice, el término en que se propuso y un
payload de comando opaco — el contenido de ese payload (p.ej. un
`LogRecord` serializado) es responsabilidad exclusiva de quien ensambla
este módulo dentro de `nanosql`, no de este repo.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

# Alias simples (no `type X = ...` de PEP 695: [tool.mypy] fija
# python_version = "3.11" y ese subconjunto de sintaxis requiere 3.12).
NodeId = str
Term = int
LogIndex = int
Command = bytes


class RaftRole(Enum):
    """Rol del nodo dentro del término actual."""

    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


@dataclass(frozen=True, slots=True)
class LogEntry:
    """Una entrada del log replicado, ya con término e índice asignados.

    `index` es el índice absoluto de Raft (1-indexado, monotónico
    creciente, nunca reutilizado aunque haya compactación por snapshot).
    `term` es el término del líder que la propuso originalmente — clave
    para las comprobaciones de consistencia de AppendEntries (§5.3 del
    paper de Raft) y para la regla de "solo comprometer entradas del
    término actual directamente" (§5.4.2).
    """

    index: LogIndex
    term: Term
    command: Command

    def __post_init__(self) -> None:
        if self.index < 1:
            raise ValueError(f"index debe ser >= 1, recibido {self.index}")
        if self.term < 1:
            raise ValueError(f"term debe ser >= 1, recibido {self.term}")


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Snapshot compactado del estado hasta (e incluyendo) `last_included_index`.

    `state` es el estado opaco devuelto por la máquina de estados
    (`StateMachine.take_snapshot`) — este módulo no interpreta su
    contenido, solo lo transporta y lo persiste en memoria.
    """

    last_included_index: LogIndex
    last_included_term: Term
    state: bytes


@dataclass(frozen=True, slots=True)
class RequestVoteRequest:
    """RPC `RequestVote` (§5.2): un candidato solicita el voto de un peer."""

    term: Term
    candidate_id: NodeId
    last_log_index: LogIndex
    last_log_term: Term


@dataclass(frozen=True, slots=True)
class RequestVoteResponse:
    """Respuesta a `RequestVote`.

    `voter_id` no forma parte del RPC real de Raft (la conexión ya
    identifica al que responde) pero aquí es necesario porque el
    transporte simulado devuelve la respuesta de vuelta al candidato sin
    contexto de conexión implícito.
    """

    term: Term
    vote_granted: bool
    voter_id: NodeId


@dataclass(frozen=True, slots=True)
class AppendEntriesRequest:
    """RPC `AppendEntries` (§5.3): replicación de entradas + heartbeat.

    Una lista de `entries` vacía es un heartbeat puro. `prev_log_index`/
    `prev_log_term` son la base sobre la que el seguidor debe verificar
    consistencia antes de aceptar `entries`.
    """

    term: Term
    leader_id: NodeId
    prev_log_index: LogIndex
    prev_log_term: Term
    entries: tuple[LogEntry, ...]
    leader_commit: LogIndex


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse:
    """Respuesta a `AppendEntries`.

    `match_index` (si `success`) le dice al líder hasta qué índice
    coincide el log del seguidor, evitando decrementar `next_index` de
    uno en uno. `conflict_index`/`conflict_term` (si no `success`)
    permiten al líder saltar directamente al primer índice del término
    en conflicto en vez de retroceder entrada a entrada — optimización
    estándar descrita en la sección 5.3 del paper (nota a pie de página).
    """

    term: Term
    success: bool
    responder_id: NodeId
    match_index: LogIndex = 0
    conflict_index: LogIndex | None = None
    conflict_term: Term | None = None


@dataclass(frozen=True, slots=True)
class InstallSnapshotRequest:
    """RPC `InstallSnapshot` (§7): usado cuando el seguidor necesita
    entradas que el líder ya compactó en un snapshot."""

    term: Term
    leader_id: NodeId
    snapshot: Snapshot


@dataclass(frozen=True, slots=True)
class InstallSnapshotResponse:
    """Respuesta a `InstallSnapshot`."""

    term: Term
    responder_id: NodeId
