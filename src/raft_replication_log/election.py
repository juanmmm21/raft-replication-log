"""Elección de líder (§5.2 del paper de Raft): timeouts aleatorizados por
nodo, términos monotónicos crecientes y la regla de "election safety"
(a lo sumo un líder por término, garantizado por voto único + mayoría).
"""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

from .models import RaftRole, RequestVoteRequest, RequestVoteResponse

if TYPE_CHECKING:
    from .node import RaftNode


def random_election_timeout(rng: random.Random, timeout_range: tuple[int, int]) -> int:
    """Sortea un timeout de elección dentro de `timeout_range` (ticks).

    `rng` se inyecta siempre desde fuera (nunca `random` del módulo
    global): es la regla B de `CLAUDE.md` — cualquier aleatoriedad debe
    ser seedeable explícitamente para que los tests sean reproducibles,
    y aquí es todavía más crítico porque el propio DoD pide timeouts
    aleatorizados como mecanismo central para evitar que dos nodos
    empiecen una elección exactamente al mismo tiempo (split vote).
    """
    low, high = timeout_range
    return rng.randint(low, high)


def start_election(node: RaftNode) -> None:
    """Transición a candidato y arranque de una nueva elección.

    Incrementar el término y auto-votarse ocurren siempre juntos, antes
    de pedir voto a los peers, para que un candidato nunca cuente su
    propio voto en un término distinto al que anuncia en el RPC.
    """
    node.current_term += 1
    node.role = RaftRole.CANDIDATE
    node.voted_for = node.node_id
    node.leader_id = None
    node.reset_votes(node.node_id)
    node.reset_election_timer()

    request = RequestVoteRequest(
        term=node.current_term,
        candidate_id=node.node_id,
        last_log_index=node.last_log_index(),
        last_log_term=node.last_log_term(),
    )
    for peer_id in node.peer_ids:
        response = node.transport.send_request_vote(node.node_id, peer_id, request)
        if response is not None:
            handle_request_vote_response(node, response)


def handle_request_vote(node: RaftNode, request: RequestVoteRequest) -> RequestVoteResponse:
    """Responde a un `RequestVote` entrante.

    Concede el voto solo si (a) todavía no ha votado en este término por
    otro candidato, y (b) el log del candidato está "al menos tan
    actualizado" como el propio (regla de "leader completeness" — un
    candidato con un log desactualizado nunca puede ganar, así que las
    entradas comprometidas nunca se pierden en una nueva elección).
    """
    if request.term > node.current_term:
        node.step_down(request.term)

    if request.term < node.current_term:
        return RequestVoteResponse(
            term=node.current_term, vote_granted=False, voter_id=node.node_id
        )

    log_is_up_to_date = request.last_log_term > node.last_log_term() or (
        request.last_log_term == node.last_log_term()
        and request.last_log_index >= node.last_log_index()
    )
    can_vote = node.voted_for is None or node.voted_for == request.candidate_id

    if can_vote and log_is_up_to_date:
        node.voted_for = request.candidate_id
        node.reset_election_timer()
        return RequestVoteResponse(term=node.current_term, vote_granted=True, voter_id=node.node_id)

    return RequestVoteResponse(term=node.current_term, vote_granted=False, voter_id=node.node_id)


def handle_request_vote_response(node: RaftNode, response: RequestVoteResponse) -> None:
    if response.term > node.current_term:
        node.step_down(response.term)
        return
    if node.role != RaftRole.CANDIDATE or response.term != node.current_term:
        return  # respuesta obsoleta (de un término anterior) o ya no somos candidatos
    if response.vote_granted:
        node.register_vote(response.voter_id)
        if node.vote_count() >= node.quorum_size():
            node.become_leader()
