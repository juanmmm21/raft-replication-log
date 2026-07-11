"""CLI de demostración para raft-replication-log.

Subcomandos:

    demo              elección de líder + replicación de varios comandos
                       hasta compromiso por quorum, en un cluster de 5 nodos.
    partition-demo     particiona el cluster en minoría/mayoría, demuestra
                       que la minoría no puede comprometer entradas ni
                       elegir líder propio, sana la partición y muestra
                       cómo el nodo minoritario se pone al día.
    snapshot-demo       replica suficientes entradas como para disparar
                       snapshotting automático y muestra el log truncado.
    benchmark          mide ticks-hasta-elección y throughput de
                       replicación para varios tamaños de cluster.
"""

from __future__ import annotations

import argparse
import sys
import time

from raft_replication_log import pipeline
from raft_replication_log.errors import RaftError
from raft_replication_log.state_machine import InMemoryStateMachine


def _run_demo() -> int:
    cluster = pipeline.build_cluster([f"node-{i}" for i in range(5)], seed=1)
    print("=== elección de líder (5 nodos, timeouts aleatorizados) ===")
    leader_id = pipeline.elect_leader(cluster)
    print(f"  líder elegido: {leader_id} (término {cluster.nodes[leader_id].current_term})")

    print()
    print("=== replicación de 3 comandos hasta compromiso por quorum ===")
    for i, payload in enumerate((b"SET x=1", b"SET y=2", b"DELETE x"), start=1):
        index = pipeline.replicate_command(cluster, payload)
        print(f"  comando {i} ({payload!r}) comprometido en índice {index}")

    # `replicate_command` solo garantiza que el LÍDER comprometió por
    # quorum (definición real de "commit" en Raft: la mayoría ya tiene la
    # entrada en su log). Que cada seguidor concreto AVANCE su propio
    # `commit_index` y aplique la entrada a su máquina de estados es
    # asíncrono: solo ocurre cuando recibe el próximo `AppendEntries` con
    # el `leader_commit` actualizado, no en el mismo mensaje que le trajo
    # la entrada (ver `replication.replicate_to_peer`). Sin estos ticks
    # extra, comprobar `applied_commands` justo después de comprometer
    # el último comando es una carrera: unos seguidores ya habrán recibido
    # ese heartbeat de seguimiento y otros no, dando un falso negativo de
    # inconsistencia — se detectó exactamente así al escribir esta demo.
    cluster.tick(cluster.nodes[leader_id].heartbeat_interval_ticks + 1)

    # La CLI de demostración siempre usa la implementación por defecto
    # (nunca una `StateMachine` custom inyectada); el `isinstance` deja
    # que mypy estreche el tipo sin necesitar `# type: ignore`.
    applied_by_node: dict[str, tuple[bytes, ...]] = {}
    for node_id, node in cluster.nodes.items():
        if isinstance(node.state_machine, InMemoryStateMachine):
            applied_by_node[node_id] = node.state_machine.applied_commands
        print(
            f"  {node_id}: role={node.role.name} commit_index={node.commit_index} "
            f"applied={list(applied_by_node.get(node_id, ()))}"
        )

    all_agree = len({commands for commands in applied_by_node.values()}) == 1
    print(f"  todos los nodos aplicaron la misma secuencia: {all_agree}")
    return 0 if all_agree else 1


def _cmd_demo(_: argparse.Namespace) -> int:
    return _run_demo()


def _run_partition_demo() -> int:
    node_ids = [f"node-{i}" for i in range(5)]
    cluster = pipeline.build_cluster(node_ids, seed=2)
    leader_id = pipeline.elect_leader(cluster)
    print(f"=== líder inicial: {leader_id} ===")
    pipeline.replicate_command(cluster, b"SET a=1")

    minority = {leader_id, node_ids[(node_ids.index(leader_id) + 1) % 5]}
    majority = set(node_ids) - minority
    print(f"=== partición: minoría {sorted(minority)} (líder) vs mayoría {sorted(majority)} ===")
    cluster.partition(minority, majority)

    cluster.tick(60)
    leaders = cluster.leaders_by_term()
    print(f"  líderes por término tras la partición: {leaders}")
    max_leaders_in_a_term = max((len(ids) for ids in leaders.values()), default=0)
    print(f"  máximo de líderes simultáneos en un mismo término: {max_leaders_in_a_term} (max. 1)")

    # Deliberadamente NO se usa `pipeline.replicate_command` aquí: esa
    # función resuelve "el líder actual" por término más alto en todo el
    # cluster (`Cluster.current_leader`), así que tras la partición
    # encontraría al líder nuevo y legítimo de la mayoría, no al líder
    # viejo aislado -- probaría la pregunta equivocada. Se propone a
    # propósito contra el nodo que sabemos que quedó de líder en la
    # minoría antes de particionar.
    old_minority_leader = cluster.nodes[leader_id]
    commit_before = old_minority_leader.commit_index
    try:
        old_minority_leader.propose(b"SET b=2 (desde la minoria vieja)")
    except RaftError:
        pass  # ya ni siquiera se cree líder: mejor todavía para el invariante
    cluster.tick(30)
    minority_committed = old_minority_leader.commit_index > commit_before
    print(f"  ¿la minoría antigua logró comprometer algo por su cuenta?: {minority_committed}")

    print("=== sanando la partición ===")
    cluster.heal_partition()
    cluster.tick(40)
    new_leader = cluster.current_leader()
    print(f"  líder tras sanar: {new_leader}")
    index = pipeline.replicate_command(cluster, b"SET c=3")
    print(f"  nuevo comando comprometido en índice {index} con todo el cluster reunificado")

    return 0 if (max_leaders_in_a_term <= 1 and not minority_committed) else 1


def _cmd_partition_demo(_: argparse.Namespace) -> int:
    return _run_partition_demo()


def _run_snapshot_demo() -> int:
    cluster = pipeline.build_cluster([f"node-{i}" for i in range(3)], seed=3, snapshot_threshold=5)
    pipeline.elect_leader(cluster)
    print("=== replicando 12 comandos con snapshot_threshold=5 ===")
    for i in range(12):
        pipeline.replicate_command(cluster, f"cmd-{i}".encode())

    for node_id, node in cluster.nodes.items():
        snap = node.snapshot
        snap_desc = f"hasta {snap.last_included_index}" if snap else "ninguno"
        summary = f"log_len={len(node.log)} snapshot={snap_desc} commit={node.commit_index}"
        print(f"  {node_id}: {summary}")

    all_snapshotted = all(node.snapshot is not None for node in cluster.nodes.values())
    logs_are_short = all(len(node.log) < 12 for node in cluster.nodes.values())
    print(f"  todos los nodos compactaron: {all_snapshotted}, logs truncados: {logs_are_short}")
    return 0 if (all_snapshotted and logs_are_short) else 1


def _cmd_snapshot_demo(_: argparse.Namespace) -> int:
    return _run_snapshot_demo()


def _benchmark_election(cluster_size: int, trials: int) -> None:
    tick_counts = []
    for seed in range(trials):
        cluster = pipeline.build_cluster([f"node-{i}" for i in range(cluster_size)], seed=seed)
        ticks = 0
        while cluster.current_leader() is None and ticks < pipeline.DEFAULT_MAX_TICKS:
            cluster.tick()
            ticks += 1
        tick_counts.append(ticks)
    avg = sum(tick_counts) / len(tick_counts)
    print(f"  nodos={cluster_size}: ticks-hasta-elección promedio={avg:.1f} (sobre {trials} seeds)")


def _benchmark_replication(cluster_size: int, num_commands: int) -> None:
    cluster = pipeline.build_cluster([f"node-{i}" for i in range(cluster_size)], seed=42)
    pipeline.elect_leader(cluster)
    started_at = time.perf_counter()
    for i in range(num_commands):
        pipeline.replicate_command(cluster, f"cmd-{i}".encode())
    elapsed = time.perf_counter() - started_at
    throughput = num_commands / elapsed if elapsed > 0 else float("inf")
    print(
        f"  nodos={cluster_size}: {num_commands} comandos comprometidos en {elapsed:.3f}s "
        f"({throughput:.0f} comandos/s)"
    )


def _cmd_benchmark(args: argparse.Namespace) -> int:
    print("=== ticks hasta elección de líder, por tamaño de cluster ===")
    for size in args.sizes:
        _benchmark_election(size, args.trials)
    print()
    print("=== throughput de replicación (comandos comprometidos por segundo) ===")
    for size in args.sizes:
        _benchmark_replication(size, args.commands)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="raft-replication-log")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "demo", help="elección de líder + replicación con quorum en un cluster sano"
    )
    subparsers.add_parser(
        "partition-demo", help="partición de red, prevención de split-brain y recuperación"
    )
    subparsers.add_parser("snapshot-demo", help="snapshotting automático truncando el log")

    benchmark = subparsers.add_parser(
        "benchmark", help="ticks-hasta-elección y throughput de replicación"
    )
    benchmark.add_argument("--sizes", type=int, nargs="+", default=[3, 5])
    benchmark.add_argument("--trials", type=int, default=20)
    benchmark.add_argument("--commands", type=int, default=200)

    return parser


_HANDLERS = {
    "demo": _cmd_demo,
    "partition-demo": _cmd_partition_demo,
    "snapshot-demo": _cmd_snapshot_demo,
    "benchmark": _cmd_benchmark,
}


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _HANDLERS[args.command]
    try:
        return handler(args)
    except RaftError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
