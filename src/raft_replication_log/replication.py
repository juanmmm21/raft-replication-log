"""Replicación de entradas de log (§5.3) y avance de `commit_index` por
quorum de mayoría.

Solo el líder de un término dado propone entradas nuevas; los seguidores
solo las aceptan tras pasar la comprobación de consistencia de
`prev_log_index`/`prev_log_term`. `_advance_commit_index` implementa
además la regla de seguridad de §5.4.2: una entrada solo se comprometé
por conteo directo de replicación si pertenece al término actual del
líder — entradas de términos anteriores se comprometen solo de forma
indirecta, cuando una entrada posterior del término actual alcanza
quorum. Sin esta regla, un líder podría "resucitar" y comprometer una
entrada de un término viejo que en realidad ya había sido sobrescrita
por la mayoría, violando la durabilidad de compromisos ya confirmados a
otros nodos.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from . import snapshot as snapshot_module
from .errors import NotLeaderError
from .models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    LogEntry,
    LogIndex,
    NodeId,
    RaftRole,
)

if TYPE_CHECKING:
    from .node import RaftNode


def propose(node: RaftNode, command: bytes) -> LogIndex:
    """Añade `command` al log local del líder (todavía sin comprometer).

    No espera al quorum: solo registra la entrada. `RaftNode.propose`
    (la API pública) es quien además dispara la replicación inmediata.
    """
    if node.role != RaftRole.LEADER:
        raise NotLeaderError(node.node_id, node.current_term, node.leader_id)
    entry = LogEntry(index=node.last_log_index() + 1, term=node.current_term, command=command)
    node.append_entries_local([entry])
    return entry.index


def replicate_to_peer(node: RaftNode, peer_id: NodeId) -> None:
    """Construye y envía a `peer_id` el RPC adecuado: `AppendEntries` si el
    líder todavía conserva las entradas que ese peer necesita a partir de
    `next_index`, o `InstallSnapshot` si ya las compactó."""
    next_idx = node.next_index.get(peer_id, node.last_log_index() + 1)
    if node.snapshot is not None and next_idx <= node.snapshot.last_included_index:
        snapshot_request = snapshot_module.build_install_snapshot_request(node)
        snapshot_response = node.transport.send_install_snapshot(
            node.node_id, peer_id, snapshot_request
        )
        if snapshot_response is not None:
            snapshot_module.handle_install_snapshot_response(node, peer_id, snapshot_response)
        return

    prev_index = next_idx - 1
    prev_term = node.term_at(prev_index) or 0
    append_request = AppendEntriesRequest(
        term=node.current_term,
        leader_id=node.node_id,
        prev_log_index=prev_index,
        prev_log_term=prev_term,
        entries=node.entries_from(next_idx),
        leader_commit=node.commit_index,
    )
    append_response = node.transport.send_append_entries(node.node_id, peer_id, append_request)
    if append_response is not None:
        handle_append_entries_response(node, peer_id, append_response)


def handle_append_entries(node: RaftNode, request: AppendEntriesRequest) -> AppendEntriesResponse:
    if request.term > node.current_term:
        node.step_down(request.term)

    if request.term < node.current_term:
        return AppendEntriesResponse(
            term=node.current_term, success=False, responder_id=node.node_id
        )

    # Un AppendEntries válido de este término reconoce al líder y resetea
    # el timeout — incluye el caso de un candidato que pierde la
    # elección al descubrir que ya hay un líder en su mismo término.
    node.role = RaftRole.FOLLOWER
    node.leader_id = request.leader_id
    node.reset_election_timer()

    prev_term = node.term_at(request.prev_log_index)
    if prev_term is None or prev_term != request.prev_log_term:
        conflict_index, conflict_term = node.conflict_hint(request.prev_log_index)
        return AppendEntriesResponse(
            term=node.current_term,
            success=False,
            responder_id=node.node_id,
            conflict_index=conflict_index,
            conflict_term=conflict_term,
        )

    _merge_entries(node, request.prev_log_index, request.entries)

    if request.leader_commit > node.commit_index:
        node.commit_index = min(request.leader_commit, node.last_log_index())
        node.apply_committed()

    return AppendEntriesResponse(
        term=node.current_term,
        success=True,
        responder_id=node.node_id,
        match_index=request.prev_log_index + len(request.entries),
    )


def _merge_entries(node: RaftNode, prev_index: LogIndex, entries: tuple[LogEntry, ...]) -> None:
    """Fusiona `entries` a partir de `prev_index + 1` (§5.3): solo trunca
    el sufijo del log local a partir del primer punto de divergencia real
    — nunca por delante de eso — para no descartar entradas ya
    comprometidas ante un reenvío duplicado de un heartbeat/AppendEntries
    con las mismas entradas que ya teníamos."""
    insert_at = prev_index + 1
    for offset, entry in enumerate(entries):
        index = insert_at + offset
        existing_term = node.term_at(index)
        if existing_term == entry.term:
            continue
        if existing_term is not None:
            node.truncate_from(index)
        node.append_entries_local(entries[offset:])
        return


def handle_append_entries_response(
    node: RaftNode, peer_id: NodeId, response: AppendEntriesResponse
) -> None:
    if response.term > node.current_term:
        node.step_down(response.term)
        return
    if node.role != RaftRole.LEADER or response.term != node.current_term:
        return  # respuesta obsoleta (de un término/rol que ya no es el actual)

    if response.success:
        node.match_index[peer_id] = max(node.match_index.get(peer_id, 0), response.match_index)
        node.next_index[peer_id] = response.match_index + 1
        _advance_commit_index(node)
        return

    if response.conflict_term is not None:
        last_idx_of_conflict_term = node.last_index_of_term(response.conflict_term)
        if last_idx_of_conflict_term is not None:
            node.next_index[peer_id] = last_idx_of_conflict_term + 1
        elif response.conflict_index is not None:
            node.next_index[peer_id] = response.conflict_index
        else:
            node.next_index[peer_id] = max(1, node.next_index.get(peer_id, 1) - 1)
    elif response.conflict_index is not None:
        node.next_index[peer_id] = response.conflict_index
    else:
        node.next_index[peer_id] = max(1, node.next_index.get(peer_id, 1) - 1)


def _advance_commit_index(node: RaftNode) -> None:
    match_indices = sorted(
        [node.last_log_index()] + [node.match_index.get(peer, 0) for peer in node.peer_ids]
    )
    candidate_index = match_indices[len(match_indices) - node.quorum_size()]
    if candidate_index > node.commit_index and node.term_at(candidate_index) == node.current_term:
        node.commit_index = candidate_index
        node.apply_committed()
