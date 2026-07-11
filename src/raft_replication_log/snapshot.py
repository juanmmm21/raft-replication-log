"""Snapshotting (§7): compacta el log ya aplicado en un `Snapshot` opaco
para poder truncarlo sin perder estado reconstruible, y el RPC
`InstallSnapshot` para ponerse al día a un seguidor demasiado atrasado
como para que el líder todavía tenga las entradas que necesita.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .errors import SnapshotOutOfRangeError
from .models import (
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogIndex,
    NodeId,
    RaftRole,
    Snapshot,
)

if TYPE_CHECKING:
    from .node import RaftNode


def create_snapshot(node: RaftNode, up_to_index: LogIndex | None = None) -> Snapshot:
    """Compacta el log hasta `up_to_index` (por defecto, `last_applied`).

    Solo se pueden compactar entradas ya aplicadas a la máquina de
    estados: comprimir una entrada todavía no comprometida perdería
    información irrecuperable si esa entrada llegase a descartarse más
    tarde (p.ej. por perder una elección frente a un log distinto).
    """
    target_index = up_to_index if up_to_index is not None else node.last_applied
    if target_index < 1 or target_index > node.last_applied:
        raise SnapshotOutOfRangeError(
            f"no se puede snapshotear hasta el índice {target_index}: "
            f"last_applied={node.last_applied}"
        )
    if node.snapshot is not None and target_index <= node.snapshot.last_included_index:
        return node.snapshot  # ya compactado hasta ahí o más allá: no-op

    target_term = node.term_at(target_index)
    if target_term is None:
        raise SnapshotOutOfRangeError(f"índice {target_index} ya fuera del log disponible")

    state = node.state_machine.take_snapshot()
    new_snapshot = Snapshot(
        last_included_index=target_index, last_included_term=target_term, state=state
    )
    node.snapshot = new_snapshot
    node.discard_before(target_index)
    return new_snapshot


def build_install_snapshot_request(node: RaftNode) -> InstallSnapshotRequest:
    if node.snapshot is None:
        raise SnapshotOutOfRangeError("no hay snapshot que instalar")
    return InstallSnapshotRequest(
        term=node.current_term, leader_id=node.node_id, snapshot=node.snapshot
    )


def handle_install_snapshot(
    node: RaftNode, request: InstallSnapshotRequest
) -> InstallSnapshotResponse:
    if request.term > node.current_term:
        node.step_down(request.term)

    if request.term < node.current_term:
        return InstallSnapshotResponse(term=node.current_term, responder_id=node.node_id)

    node.role = RaftRole.FOLLOWER
    node.leader_id = request.leader_id
    node.reset_election_timer()

    incoming = request.snapshot
    if (
        node.snapshot is not None
        and incoming.last_included_index <= node.snapshot.last_included_index
    ):
        return InstallSnapshotResponse(
            term=node.current_term, responder_id=node.node_id
        )  # ya al día

    # Si el seguidor ya tenía, en su propio log, exactamente esa entrada
    # (mismo índice y término), conserva el sufijo posterior — sigue
    # siendo válido y evita perder replicación ya hecha. Si diverge, todo
    # el log local se descarta: el snapshot es ahora la única fuente de
    # verdad hasta `last_included_index`.
    existing_term = node.term_at(incoming.last_included_index)
    if existing_term != incoming.last_included_term:
        node.log = []

    node.state_machine.restore_snapshot(incoming.state)
    node.snapshot = incoming
    node.discard_before(incoming.last_included_index)
    node.commit_index = max(node.commit_index, incoming.last_included_index)
    node.last_applied = incoming.last_included_index

    return InstallSnapshotResponse(term=node.current_term, responder_id=node.node_id)


def handle_install_snapshot_response(
    node: RaftNode, peer_id: NodeId, response: InstallSnapshotResponse
) -> None:
    if response.term > node.current_term:
        node.step_down(response.term)
        return
    if node.role != RaftRole.LEADER or response.term != node.current_term or node.snapshot is None:
        return
    node.match_index[peer_id] = max(
        node.match_index.get(peer_id, 0), node.snapshot.last_included_index
    )
    node.next_index[peer_id] = node.snapshot.last_included_index + 1
