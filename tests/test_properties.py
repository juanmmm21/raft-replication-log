"""Tests de propiedades: secuencias aleatorias de operaciones (semilla
fija) contra un cluster real, verificando en cada paso -- no solo al
final -- las invariantes de seguridad formales del paper de Raft (§5.4.3):
election safety, log matching y state machine safety.

Nota sobre el "modelo independiente": a diferencia de, por ejemplo,
comparar un optimizador de reordenación de joins contra fuerza bruta
sobre las `n!` alternativas, un algoritmo de consenso no tiene un
"modelo de referencia" trivial más simple que sí mismo con el que
comparar su salida. El equivalente aquí -- y lo que este módulo verifica
en cada paso de cada secuencia aleatoria -- son las propias invariantes
de seguridad formales del algoritmo, que son independientes de los
detalles de esta implementación concreta: si se violan, hay un bug, sin
importar qué decisión interna lo causó.
"""

from __future__ import annotations

import random

import pytest

from raft_replication_log.cluster import Cluster
from raft_replication_log.errors import RaftError
from raft_replication_log.models import LogIndex
from raft_replication_log.state_machine import InMemoryStateMachine

NODE_IDS = [f"node-{i}" for i in range(5)]


def _check_election_safety(cluster: Cluster) -> None:
    for term, leaders in cluster.leaders_by_term().items():
        assert len(leaders) <= 1, f"election safety violada en término {term}: {leaders}"


def _check_log_matching(cluster: Cluster) -> None:
    """Si dos nodos tienen una entrada con el mismo índice y término, todas
    las entradas anteriores a esa deben ser idénticas -- se comprueba aquí
    de forma más directa (misma índice+término implica mismo comando),
    que es la consecuencia observable de log matching entrada a entrada."""
    nodes = list(cluster.nodes.values())
    for i, node_a in enumerate(nodes):
        for node_b in nodes[i + 1 :]:
            last_common = min(node_a.last_log_index(), node_b.last_log_index())
            for index in range(1, last_common + 1):
                term_a = node_a.term_at(index)
                term_b = node_b.term_at(index)
                if term_a is None or term_b is None or term_a != term_b:
                    continue
                entry_a = node_a.entry_at(index)
                entry_b = node_b.entry_at(index)
                if entry_a is None or entry_b is None:
                    continue  # uno de los dos ya lo compactó en snapshot
                assert entry_a.command == entry_b.command, (
                    f"log matching violado en índice {index}: "
                    f"{node_a.node_id}={entry_a.command!r} vs {node_b.node_id}={entry_b.command!r}"
                )


def _check_state_machine_safety(cluster: Cluster, applied_at_index: dict[LogIndex, bytes]) -> None:
    """Si dos nodos (en cualquier momento de todo el escenario, no solo al
    final) aplican una entrada al mismo índice, debe ser exactamente el
    mismo comando. Se acumula en `applied_at_index` a través de toda la
    ejecución porque un nodo puede aplicar y más tarde compactar esa
    entrada (perdiéndola de `log`, pero sin que eso invalide lo que ya se
    aplicó)."""
    for node in cluster.nodes.values():
        if not isinstance(node.state_machine, InMemoryStateMachine):
            continue
        for index, command in enumerate(node.state_machine.applied_commands, start=1):
            if index in applied_at_index:
                assert applied_at_index[index] == command, (
                    f"state machine safety violada en índice {index}: "
                    f"{applied_at_index[index]!r} (ya visto) vs {command!r} "
                    f"(nodo {node.node_id})"
                )
            else:
                applied_at_index[index] = command


_ACTIONS = ("tick", "tick", "tick", "propose", "partition", "heal", "crash", "recover")


def _run_random_scenario(seed: int, num_steps: int = 500) -> None:
    scenario_rng = random.Random(seed)
    cluster = Cluster(
        NODE_IDS,
        rng=random.Random(seed + 1000),
        election_timeout_ticks=(8, 16),
        heartbeat_interval_ticks=3,
        snapshot_threshold=6,
    )
    applied_at_index: dict[LogIndex, bytes] = {}
    command_counter = 0

    for _ in range(num_steps):
        action = scenario_rng.choice(_ACTIONS)

        if action == "tick":
            cluster.tick(scenario_rng.randint(1, 3))
        elif action == "propose":
            leader_id = cluster.current_leader()
            if leader_id is not None:
                command_counter += 1
                try:
                    cluster.nodes[leader_id].propose(f"cmd-{command_counter}".encode())
                except RaftError:
                    pass
        elif action == "partition":
            alive = [n for n in NODE_IDS if not cluster.is_down(n)]
            if len(alive) >= 3:
                scenario_rng.shuffle(alive)
                split = scenario_rng.randint(1, len(alive) - 1)
                cluster.partition(alive[:split], alive[split:])
        elif action == "heal":
            cluster.heal_partition()
        elif action == "crash":
            candidates = [n for n in NODE_IDS if not cluster.is_down(n)]
            if candidates:
                cluster.crash_node(scenario_rng.choice(candidates))
        elif action == "recover":
            down = [n for n in NODE_IDS if cluster.is_down(n)]
            if down:
                cluster.recover_node(scenario_rng.choice(down))

        _check_election_safety(cluster)
        _check_log_matching(cluster)
        _check_state_machine_safety(cluster, applied_at_index)

    # Sanar todo y dejar converger al final: confirma que ninguna
    # divergencia quedó escondida solo porque el cluster seguía roto.
    cluster.heal_partition()
    for node_id in NODE_IDS:
        cluster.recover_node(node_id)
    cluster.tick(300)

    _check_election_safety(cluster)
    _check_log_matching(cluster)
    _check_state_machine_safety(cluster, applied_at_index)

    if cluster.current_leader() is not None and command_counter > 0:
        # con el cluster sano de nuevo, todos los nodos vivos deben
        # converger exactamente a la misma secuencia aplicada.
        reference: tuple[bytes, ...] | None = None
        for node in cluster.nodes.values():
            if not isinstance(node.state_machine, InMemoryStateMachine):
                continue
            if reference is None:
                reference = node.state_machine.applied_commands
            else:
                assert node.state_machine.applied_commands == reference, (
                    f"nodo {node.node_id} no convergió tras sanar el cluster"
                )


@pytest.mark.parametrize("seed", range(15))
def test_random_scenario_preserves_safety_invariants(seed: int) -> None:
    _run_random_scenario(seed)
