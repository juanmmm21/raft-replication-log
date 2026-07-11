"""`RaftNode`: estado de un nodo del cluster y orquestación de su ciclo de vida.

La lógica de cada sub-dominio (elección, replicación, snapshotting) vive
en `election.py`/`replication.py`/`snapshot.py` como funciones que reciben
el nodo como primer argumento y operan sobre sus atributos — el mismo
patrón que separar `joins.py`/`aggregate.py`/`sort.py` de `operators.py`
en `vectorized-execution-engine`. Los métodos de `RaftNode` sin guion bajo
inicial que no forman parte de la API pública documentada en el README
(`step_down`, `become_leader`, `apply_committed`...) son superficie
compartida entre esos módulos y este: no se marcan privados con `_`
porque Python no distingue "privado del paquete" de "privado de la
clase" y forzar el prefijo aquí solo estorbaría a los módulos hermanos
que los llaman constantemente. Solo `_offset_of` es realmente privado de
este archivo.
"""

from __future__ import annotations

import random
from collections.abc import Iterable

from . import election, replication
from . import snapshot as snapshot_module
from .models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    LogIndex,
    NodeId,
    RaftRole,
    RequestVoteRequest,
    RequestVoteResponse,
    Snapshot,
    Term,
)
from .protocols import StateMachine, Transport
from .state_machine import InMemoryStateMachine

# Por debajo de este umbral entre el intervalo de heartbeat y el mínimo
# de timeout de elección, un líder sano nunca dispararía una elección
# espuria en sus propios seguidores (regla práctica estándar de Raft).
_MIN_TIMEOUT_TO_HEARTBEAT_RATIO = 2


class RaftNode:
    """Un nodo del cluster Raft simulado."""

    def __init__(
        self,
        node_id: NodeId,
        peer_ids: Iterable[NodeId],
        transport: Transport,
        rng: random.Random,
        state_machine: StateMachine | None = None,
        election_timeout_ticks: tuple[int, int] = (10, 20),
        heartbeat_interval_ticks: int = 3,
        snapshot_threshold: int | None = None,
    ) -> None:
        if election_timeout_ticks[0] < heartbeat_interval_ticks * _MIN_TIMEOUT_TO_HEARTBEAT_RATIO:
            raise ValueError(
                "election_timeout_ticks mínimo debe ser al menos "
                f"{_MIN_TIMEOUT_TO_HEARTBEAT_RATIO}x heartbeat_interval_ticks, "
                f"recibido timeout={election_timeout_ticks} heartbeat={heartbeat_interval_ticks}"
            )
        self.node_id = node_id
        self.peer_ids = tuple(peer_ids)
        self.transport = transport
        self.rng = rng
        self.state_machine: StateMachine = state_machine or InMemoryStateMachine()
        self.election_timeout_range = election_timeout_ticks
        self.heartbeat_interval_ticks = heartbeat_interval_ticks
        self.snapshot_threshold = snapshot_threshold

        # Estado persistente en Raft real (§5.1). Este módulo no escribe a
        # disco (ver README: protocolo puro sobre cluster en memoria), pero
        # conceptualmente estos tres campos son los que un nodo real jamás
        # puede perder sin arriesgar votar dos veces en el mismo término.
        self.current_term: Term = 0
        self.voted_for: NodeId | None = None
        self.log: list[LogEntry] = []
        self.snapshot: Snapshot | None = None

        # Estado volátil.
        self.commit_index: LogIndex = 0
        self.last_applied: LogIndex = 0
        self.role: RaftRole = RaftRole.FOLLOWER
        self.leader_id: NodeId | None = None

        # Estado volátil de líder (reconstruido en `become_leader`).
        self.next_index: dict[NodeId, LogIndex] = {}
        self.match_index: dict[NodeId, LogIndex] = {}

        self._election_elapsed = 0
        self._heartbeat_elapsed = 0
        self._votes_received: set[NodeId] = set()
        self._election_timeout = election.random_election_timeout(
            self.rng, self.election_timeout_range
        )

    # -- tamaño de cluster --

    def quorum_size(self) -> int:
        total_nodes = len(self.peer_ids) + 1
        return total_nodes // 2 + 1

    # -- navegación del log con límite de snapshot --

    def last_log_index(self) -> LogIndex:
        if self.log:
            return self.log[-1].index
        return self.snapshot.last_included_index if self.snapshot else 0

    def last_log_term(self) -> Term:
        if self.log:
            return self.log[-1].term
        return self.snapshot.last_included_term if self.snapshot else 0

    def _offset_of(self, index: LogIndex) -> int | None:
        if not self.log:
            return None
        offset = index - self.log[0].index
        if offset < 0 or offset >= len(self.log):
            return None
        return offset

    def term_at(self, index: LogIndex) -> Term | None:
        if index == 0:
            return 0
        if self.snapshot is not None and index == self.snapshot.last_included_index:
            return self.snapshot.last_included_term
        offset = self._offset_of(index)
        return self.log[offset].term if offset is not None else None

    def entry_at(self, index: LogIndex) -> LogEntry | None:
        offset = self._offset_of(index)
        return self.log[offset] if offset is not None else None

    def entries_from(self, index: LogIndex) -> tuple[LogEntry, ...]:
        offset = self._offset_of(index)
        if offset is None:
            return ()
        return tuple(self.log[offset:])

    def truncate_from(self, index: LogIndex) -> None:
        offset = self._offset_of(index)
        if offset is not None:
            del self.log[offset:]

    def append_entries_local(self, entries: Iterable[LogEntry]) -> None:
        self.log.extend(entries)

    def discard_before(self, index: LogIndex) -> None:
        """Elimina del log en memoria las entradas ya cubiertas por un
        snapshot (`entry.index <= index`). Filtro en vez de aritmética de
        offsets: más simple de razonar y el tamaño del log en esta
        simulación nunca justifica optimizar este caso."""
        self.log = [entry for entry in self.log if entry.index > index]

    def last_index_of_term(self, term: Term) -> LogIndex | None:
        for entry in reversed(self.log):
            if entry.term == term:
                return entry.index
            if entry.term < term:
                break
        return None

    def conflict_hint(self, prev_index: LogIndex) -> tuple[LogIndex | None, Term | None]:
        """Optimización de retroceso rápido descrita en la nota a pie de
        página de §5.3: en vez de que el líder decremente `next_index` de
        uno en uno tras cada rechazo, el seguidor indica directamente el
        primer índice de su log a partir del cual el líder debería
        reintentar."""
        if prev_index > self.last_log_index():
            return self.last_log_index() + 1, None
        term = self.term_at(prev_index)
        if term is None:
            first_available = self.snapshot.last_included_index + 1 if self.snapshot else 1
            return first_available, None
        first_index_of_term = prev_index
        offset = self._offset_of(first_index_of_term)
        while offset is not None and offset > 0 and self.log[offset - 1].term == term:
            offset -= 1
            first_index_of_term -= 1
        return first_index_of_term, term

    # -- transiciones de rol --

    def step_down(self, term: Term) -> None:
        self.current_term = term
        self.role = RaftRole.FOLLOWER
        self.voted_for = None
        self.leader_id = None
        self._votes_received = set()
        self.reset_election_timer()

    def become_leader(self) -> None:
        self.role = RaftRole.LEADER
        self.leader_id = self.node_id
        self.next_index = {peer: self.last_log_index() + 1 for peer in self.peer_ids}
        self.match_index = {peer: 0 for peer in self.peer_ids}
        self._heartbeat_elapsed = 0
        # Entrada no-op de término (§5.4.2 del paper de Raft): un líder
        # nuevo solo puede comprometer directamente entradas de SU PROPIO
        # término (regla de seguridad en `replication._advance_commit_index`);
        # entradas de términos anteriores solo se comprometen de forma
        # indirecta, cuando una entrada del término actual alcanza quorum.
        # Sin este no-op, un líder que hereda un log con entradas sin
        # comprometer de un término viejo se queda bloqueado para siempre
        # si no llega ningún cliente a proponer nada nuevo -- se detectó
        # así, con un cluster que dejaba de progresar tras converger todos
        # los reintentos de un test de propiedades sin más propuestas.
        replication.propose(self, b"")
        self.send_append_entries_to_all_peers()

    def reset_election_timer(self) -> None:
        self._election_elapsed = 0
        self._election_timeout = election.random_election_timeout(
            self.rng, self.election_timeout_range
        )

    def register_vote(self, voter_id: NodeId) -> None:
        self._votes_received.add(voter_id)

    def reset_votes(self, initial_voter: NodeId) -> None:
        self._votes_received = {initial_voter}

    def vote_count(self) -> int:
        return len(self._votes_received)

    # -- aplicación a la máquina de estados + snapshot automático --

    def apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.entry_at(self.last_applied)
            # `command == b""` marca la entrada no-op que `become_leader`
            # añade a cada término nuevo (ver su docstring): existe solo
            # para poder comprometer indirectamente entradas de términos
            # anteriores, nunca representa un comando real de cliente, así
            # que no se envía a la máquina de estados.
            if entry is not None and entry.command != b"":
                self.state_machine.apply(entry.command)
        if self.snapshot_threshold is not None:
            already_compacted = self.snapshot.last_included_index if self.snapshot else 0
            if self.last_applied - already_compacted >= self.snapshot_threshold:
                snapshot_module.create_snapshot(self, self.last_applied)

    # -- replicación saliente --

    def send_append_entries_to_all_peers(self) -> None:
        for peer_id in self.peer_ids:
            replication.replicate_to_peer(self, peer_id)

    # -- API pública --

    def tick(self) -> None:
        """Avanza el reloj lógico del nodo un paso. Ver `cluster.Cluster.tick`
        para el bucle que invoca esto sobre todos los nodos del cluster."""
        if self.role == RaftRole.LEADER:
            self._heartbeat_elapsed += 1
            if self._heartbeat_elapsed >= self.heartbeat_interval_ticks:
                self._heartbeat_elapsed = 0
                self.send_append_entries_to_all_peers()
        else:
            self._election_elapsed += 1
            if self._election_elapsed >= self._election_timeout:
                election.start_election(self)

    def propose(self, command: bytes) -> LogIndex:
        """Añade `command` al log si este nodo es líder y dispara su
        replicación inmediata (no espera al próximo heartbeat). Lanza
        `errors.NotLeaderError` si no lo es."""
        index = replication.propose(self, command)
        self.send_append_entries_to_all_peers()
        return index

    def create_snapshot(self, up_to_index: LogIndex | None = None) -> Snapshot:
        return snapshot_module.create_snapshot(self, up_to_index)

    def handle_request_vote(self, request: RequestVoteRequest) -> RequestVoteResponse:
        return election.handle_request_vote(self, request)

    def handle_append_entries(self, request: AppendEntriesRequest) -> AppendEntriesResponse:
        return replication.handle_append_entries(self, request)

    def handle_install_snapshot(self, request: InstallSnapshotRequest) -> InstallSnapshotResponse:
        return snapshot_module.handle_install_snapshot(self, request)

    def __repr__(self) -> str:
        return (
            f"RaftNode(node_id={self.node_id!r}, role={self.role.name}, "
            f"term={self.current_term}, log_len={len(self.log)}, "
            f"commit_index={self.commit_index})"
        )
