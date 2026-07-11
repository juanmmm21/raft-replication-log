"""Interfaces `Protocol` pluggable del módulo, mismo patrón que
`ErrorFormatter` en `sql-query-parser` o `CostModel` en
`cost-based-query-optimizer`: un `Protocol` con una implementación por
defecto documentada, para que `nanosql` pueda sustituirla sin tocar el
núcleo del algoritmo.
"""

from __future__ import annotations

from typing import Protocol

from .models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    NodeId,
    RequestVoteRequest,
    RequestVoteResponse,
)


class Transport(Protocol):
    """Canal de RPC entre nodos del cluster.

    Decisión de diseño (documentada también en `cluster.py`): este
    ecosistema simula concurrencia de formas distintas según el
    subproyecto — hilos reales en `lock-manager-deadlock-detector`,
    estructuras en memoria puras en otros. Aquí no hace falta ni sockets
    reales ni `asyncio`: el cluster completo vive en un solo proceso y un
    solo hilo, así que el "envío" de un RPC es una llamada de función
    síncrona que devuelve la respuesta inmediatamente (o `None` si el
    transporte decide descartar el mensaje, simulando una partición de
    red o un nodo caído). No hay estado mutable compartido entre threads
    que proteger — el propio determinismo de la simulación depende de
    que todo ocurra en un orden de llamadas único y reproducible.
    """

    def send_request_vote(
        self, sender: NodeId, target: NodeId, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        """Envía `RequestVote` a `target`. `None` si el mensaje se pierde."""
        ...

    def send_append_entries(
        self, sender: NodeId, target: NodeId, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        """Envía `AppendEntries` a `target`. `None` si el mensaje se pierde."""
        ...

    def send_install_snapshot(
        self, sender: NodeId, target: NodeId, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse | None:
        """Envía `InstallSnapshot` a `target`. `None` si el mensaje se pierde."""
        ...


class StateMachine(Protocol):
    """Máquina de estados a la que se aplican las entradas comprometidas.

    Raft en sí mismo es agnóstico al contenido de `Command` — solo
    garantiza que todos los nodos aplican la misma secuencia de comandos
    en el mismo orden. `take_snapshot`/`restore_snapshot` permiten
    truncar el log sin perder estado reconstruible (DoD de snapshotting).
    """

    def apply(self, command: bytes) -> None:
        """Aplica un comando ya comprometido por quorum, en orden de índice."""
        ...

    def take_snapshot(self) -> bytes:
        """Serializa el estado actual para poder compactar el log."""
        ...

    def restore_snapshot(self, state: bytes) -> None:
        """Reemplaza el estado actual por el contenido de un snapshot recibido."""
        ...
