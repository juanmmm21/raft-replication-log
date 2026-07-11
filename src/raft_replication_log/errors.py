"""Jerarquía de excepciones propia del módulo."""

from __future__ import annotations

from .models import NodeId, Term


class RaftError(Exception):
    """Error base de todo el módulo."""


class NotLeaderError(RaftError):
    """Se intentó proponer una entrada en un nodo que no es líder.

    `known_leader` es la mejor información que el nodo tiene sobre quién
    es el líder actual (puede ser `None` si el propio nodo no lo sabe,
    p.ej. en medio de una elección).
    """

    def __init__(self, node_id: NodeId, term: Term, known_leader: NodeId | None) -> None:
        super().__init__(
            f"nodo {node_id!r} no es líder en el término {term} (líder conocido: {known_leader!r})"
        )
        self.node_id = node_id
        self.term = term
        self.known_leader = known_leader


class StaleTermError(RaftError):
    """Un mensaje o una operación referencian un término ya superado."""


class SnapshotOutOfRangeError(RaftError):
    """Se pidió compactar o instalar un snapshot con un índice inválido
    (p.ej. anterior al snapshot ya instalado, o posterior a `commit_index`)."""
