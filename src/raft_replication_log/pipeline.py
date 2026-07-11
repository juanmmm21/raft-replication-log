"""Orquestación de alto nivel sobre `Cluster`: construir un cluster con
seed explícita y esperar a que ocurran eventos multi-tick (elección
estable, un comando comprometido por quorum). Es el único punto del
módulo que combina "avanzar ticks hasta que pase X" — el resto del
código (`node.py`/`election.py`/`replication.py`/`snapshot.py`) solo
avanza un tick o procesa un único mensaje cada vez.
"""

from __future__ import annotations

import random
from collections.abc import Iterable

from .cluster import Cluster
from .errors import RaftError
from .models import LogIndex, NodeId, RaftRole

DEFAULT_MAX_TICKS = 200


def build_cluster(
    node_ids: Iterable[NodeId],
    seed: int,
    election_timeout_ticks: tuple[int, int] = (10, 20),
    heartbeat_interval_ticks: int = 3,
    snapshot_threshold: int | None = None,
) -> Cluster:
    """Crea un `Cluster` con un `random.Random` seedeado explícitamente.

    Nunca se usa el módulo `random` global sin seed (regla B de
    `CLAUDE.md`, más crítica aquí porque el propio DoD pide timeouts de
    elección aleatorizados): toda la aleatoriedad del cluster —incluida
    la de cada `RaftNode` individual— cuelga de esta única instancia.
    """
    rng = random.Random(seed)
    return Cluster(
        node_ids,
        rng=rng,
        election_timeout_ticks=election_timeout_ticks,
        heartbeat_interval_ticks=heartbeat_interval_ticks,
        snapshot_threshold=snapshot_threshold,
    )


def elect_leader(cluster: Cluster, max_ticks: int = DEFAULT_MAX_TICKS) -> NodeId:
    """Avanza el cluster tick a tick hasta que emerja un líder único y
    estable (ningún otro nodo compite en un término igual o mayor).

    Lanza `RaftError` si no converge dentro de `max_ticks` — con timeouts
    aleatorizados y sin dos nodos empezando el mismo tick, siempre debería
    converger en pocas rondas de elección; no converger indica un cluster
    mal configurado (p.ej. menos de quorum de nodos vivos).
    """
    for _ in range(max_ticks):
        cluster.tick()
        leader_id = cluster.current_leader()
        if leader_id is not None:
            return leader_id
    raise RaftError(f"no emergió ningún líder tras {max_ticks} ticks")


def replicate_command(
    cluster: Cluster, command: bytes, max_ticks: int = DEFAULT_MAX_TICKS
) -> LogIndex:
    """Propone `command` contra el líder actual (eligiendo uno primero si
    hace falta) y avanza el cluster hasta que quede comprometido
    (`commit_index` del líder alcanza el índice de la entrada) por
    quorum de mayoría.
    """
    leader_id = cluster.current_leader() or elect_leader(cluster, max_ticks)
    leader = cluster.nodes[leader_id]
    index = leader.propose(command)

    for _ in range(max_ticks):
        if leader.role == RaftRole.LEADER and leader.commit_index >= index:
            return index
        cluster.tick()
        # El líder pudo perder su rol entre medias (p.ej. partición); si
        # cambia, no hay garantía de que la entrada se llegue a comprometer
        # con este líder concreto, así que se reintenta contra el nuevo.
        current = cluster.current_leader()
        if current is not None and current != leader_id:
            return replicate_command(cluster, command, max_ticks)

    raise RaftError(f"comando no comprometido tras {max_ticks} ticks (índice propuesto {index})")
