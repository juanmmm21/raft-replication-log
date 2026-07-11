"""Implementación por defecto de `protocols.StateMachine`.

Este módulo (y Raft en general) es agnóstico al contenido de los
comandos aplicados — `nanosql` sustituirá esta implementación por una
que de verdad mute el estado de una tabla. Aquí basta con acumular los
comandos aplicados en orden, que es lo único que los tests de propiedades
necesitan para comparar el estado resultante entre nodos.
"""

from __future__ import annotations


class InMemoryStateMachine:
    """Máquina de estados de referencia: registra cada comando aplicado.

    El formato de snapshot es deliberadamente simple y propio (longitud
    en 4 bytes big-endian + payload, repetido) en vez de `pickle`: evita
    ejecutar código arbitrario al restaurar un snapshot recibido de la
    red, y es determinista entre versiones de Python.
    """

    def __init__(self) -> None:
        self._applied: list[bytes] = []

    def apply(self, command: bytes) -> None:
        self._applied.append(command)

    @property
    def applied_commands(self) -> tuple[bytes, ...]:
        return tuple(self._applied)

    def take_snapshot(self) -> bytes:
        parts: list[bytes] = []
        for command in self._applied:
            parts.append(len(command).to_bytes(4, "big"))
            parts.append(command)
        return b"".join(parts)

    def restore_snapshot(self, state: bytes) -> None:
        commands: list[bytes] = []
        offset = 0
        while offset < len(state):
            length = int.from_bytes(state[offset : offset + 4], "big")
            offset += 4
            commands.append(state[offset : offset + length])
            offset += length
        self._applied = commands
