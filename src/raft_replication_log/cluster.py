"""Cluster Raft simulado en memoria: 3-5 nodos `RaftNode` en el mismo
proceso, comunicados exclusivamente a través de `Cluster` como única
implementación de `protocols.Transport`.

Decisión de diseño explícita (DoD: "cluster simulado en memoria... sin
sockets reales ni async"): todo el cluster vive en un único hilo y
proceso. `Cluster.tick()` avanza el reloj lógico de todos los nodos vivos
en un único paso; cada RPC que un nodo "envía" es, en realidad, una
llamada de función síncrona (`RaftNode.handle_*`) que devuelve la
respuesta inmediatamente. No hay estado mutable compartido entre hilos
que proteger (a diferencia de `lock-manager-deadlock-detector`, que sí
usa hilos reales para modelar transacciones concurrentes genuinas) — la
propia determinismo de esta simulación depende de que exista un único
orden de ejecución, reproducible bit a bit dado el mismo seed.

Partición de red: `partition()` divide los nodos en grupos que no pueden
alcanzarse entre sí; cualquier RPC entre dos nodos de grupos distintos
se descarta silenciosamente (`None`), exactamente lo que un nodo real
vería como un timeout de RPC. Como el quorum siempre se calcula sobre el
tamaño *total* del cluster (`RaftNode.quorum_size`), nunca sobre el
tamaño del grupo alcanzable, una minoría aislada jamás puede reunir
mayoría de votos ni de replicación — así se evita el split-brain sin
lógica especial de detección de partición: es una consecuencia directa
de la aritmética de quorum de Raft.

Caída de nodo: `crash_node()`/`recover_node()` simulan que un nodo deja
de responder y de avanzar su reloj — equivalente, desde el punto de
vista del protocolo, a una partición de un único nodo. No se resetea
ningún estado al "recuperarlo": este módulo no persiste nada a disco, así
que no hay amnesia que simular (nunca se pierde `current_term`/
`voted_for`/`log`, que es justo lo que un nodo real garantiza mediante
escritura a almacenamiento estable antes de responder cualquier RPC).
"""

from __future__ import annotations

import random
from collections.abc import Callable, Iterable

from .models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    NodeId,
    RaftRole,
    RequestVoteRequest,
    RequestVoteResponse,
)
from .node import RaftNode
from .protocols import StateMachine
from .state_machine import InMemoryStateMachine

MIN_CLUSTER_SIZE = 3
MAX_CLUSTER_SIZE = 5


class Cluster:
    """Cluster simulado que implementa `protocols.Transport`."""

    def __init__(
        self,
        node_ids: Iterable[NodeId],
        rng: random.Random,
        election_timeout_ticks: tuple[int, int] = (10, 20),
        heartbeat_interval_ticks: int = 3,
        snapshot_threshold: int | None = None,
        state_machine_factory: Callable[[], StateMachine] | None = None,
    ) -> None:
        ids = tuple(node_ids)
        if not MIN_CLUSTER_SIZE <= len(ids) <= MAX_CLUSTER_SIZE:
            raise ValueError(
                f"un cluster Raft simulado admite entre {MIN_CLUSTER_SIZE} y "
                f"{MAX_CLUSTER_SIZE} nodos (DoD), recibidos {len(ids)}"
            )
        if len(set(ids)) != len(ids):
            raise ValueError(f"node_ids duplicados: {ids}")

        self._rng = rng
        self._down: set[NodeId] = set()
        self._partitions: tuple[frozenset[NodeId], ...] = ()

        factory: Callable[[], StateMachine] = state_machine_factory or InMemoryStateMachine
        self.nodes: dict[NodeId, RaftNode] = {
            node_id: RaftNode(
                node_id=node_id,
                peer_ids=[peer for peer in ids if peer != node_id],
                transport=self,
                rng=rng,
                state_machine=factory(),
                election_timeout_ticks=election_timeout_ticks,
                heartbeat_interval_ticks=heartbeat_interval_ticks,
                snapshot_threshold=snapshot_threshold,
            )
            for node_id in ids
        }

    def tick(self, times: int = 1) -> None:
        for _ in range(times):
            for node_id, node in self.nodes.items():
                if node_id not in self._down:
                    node.tick()

    # -- simulación de fallos --

    def crash_node(self, node_id: NodeId) -> None:
        self._down.add(node_id)

    def recover_node(self, node_id: NodeId) -> None:
        self._down.discard(node_id)

    def is_down(self, node_id: NodeId) -> bool:
        return node_id in self._down

    def partition(self, *groups: Iterable[NodeId]) -> None:
        """Divide el cluster en grupos mutuamente inalcanzables. Cualquier
        nodo que no aparezca en ningún grupo explícito queda en su propio
        grupo de un solo elemento (totalmente aislado del resto)."""
        frozen_groups = [frozenset(group) for group in groups]
        covered = {node_id for group in frozen_groups for node_id in group}
        for node_id in self.nodes:
            if node_id not in covered:
                frozen_groups.append(frozenset({node_id}))
        self._partitions = tuple(frozen_groups)

    def heal_partition(self) -> None:
        self._partitions = ()

    def _reachable(self, sender: NodeId, target: NodeId) -> bool:
        if sender in self._down or target in self._down:
            return False
        if not self._partitions:
            return True
        return any(sender in group and target in group for group in self._partitions)

    # -- protocols.Transport --

    def send_request_vote(
        self, sender: NodeId, target: NodeId, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        if not self._reachable(sender, target):
            return None
        return self.nodes[target].handle_request_vote(request)

    def send_append_entries(
        self, sender: NodeId, target: NodeId, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        if not self._reachable(sender, target):
            return None
        return self.nodes[target].handle_append_entries(request)

    def send_install_snapshot(
        self, sender: NodeId, target: NodeId, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse | None:
        if not self._reachable(sender, target):
            return None
        return self.nodes[target].handle_install_snapshot(request)

    # -- observación (usado por CLI y tests) --

    def leaders_by_term(self) -> dict[int, set[NodeId]]:
        """Agrupa los líderes vivos por término — el invariante central de
        "election safety" es que cada valor de este dict tenga tamaño <= 1."""
        result: dict[int, set[NodeId]] = {}
        for node in self.nodes.values():
            if node.role == RaftRole.LEADER:
                result.setdefault(node.current_term, set()).add(node.node_id)
        return result

    def current_leader(self) -> NodeId | None:
        """El líder utilizable ahora mismo: excluye nodos caídos, a
        diferencia de `leaders_by_term` (que sí los incluye a propósito,
        porque un nodo caído que todavía "cree" ser líder de un término
        viejo es justo lo que el chequeo de split-brain necesita poder
        ver). Un nodo caído nunca deja de tener `role == LEADER` por sí
        mismo mientras esté caído (no recibe ticks ni RPCs que lo hagan
        cambiar de idea), así que sin este filtro un líder muerto seguiría
        "reportándose" como el líder del cluster para siempre."""
        leaders = [
            node
            for node_id, node in self.nodes.items()
            if node.role == RaftRole.LEADER and node_id not in self._down
        ]
        if not leaders:
            return None
        return max(leaders, key=lambda node: node.current_term).node_id
