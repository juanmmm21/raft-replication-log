"""Raft sobre TCP real: cada nodo es un proceso (o hilo) con su propio
socket de escucha, log persistente en disco y configuración estática de
3-5 miembros.

Cómo encaja con el núcleo síncrono del protocolo sin reescribirlo:

* `TcpTransport` implementa `protocols.Transport` de forma **asíncrona**:
  `send_*` sólo encola el mensaje y devuelve `None` — exactamente lo que el
  núcleo ya interpreta como "respuesta aún no recibida / perdida". Cuando
  la respuesta llega por la red, se entrega a los mismos manejadores que
  usa la simulación (`handle_append_entries_response`, ...), que ya
  descartan respuestas obsoletas por término. Es el modelo de mensajes de
  un Raft real; la simulación en proceso (`cluster.py`) no cambia.
* Un único `RLock` por nodo serializa TODA mutación de su estado: el tick
  periódico, cada RPC entrante, cada respuesta y cada propuesta de
  cliente. Nunca se hace E/S de red con el lock tomado (sólo se encola),
  así que dos nodos que se envían mensajes a la vez no pueden bloquearse
  mutuamente.
* Al final de cada operación, con el lock aún tomado, `_after()` persiste
  con `DiskStorage.sync` (fsync) y SÓLO DESPUÉS entrega los mensajes
  encolados a los enlaces de red y responde al RPC entrante: el estado
  persistente llega a disco antes de que nadie pueda observarlo (§5.1).
"""

from __future__ import annotations

import logging
import queue
import random
import socket
import socketserver
import threading
from collections.abc import Callable, Mapping
from typing import TypeVar

from . import election, replication
from . import snapshot as snapshot_module
from .codec import CodecError, Message, decode_payload, encode_frame, frame_length
from .models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogIndex,
    NodeId,
    RaftRole,
    RequestVoteRequest,
    RequestVoteResponse,
    Term,
)
from .node import RaftNode
from .protocols import StateMachine
from .storage import DiskStorage, restore_node

log = logging.getLogger("raft_replication_log.network")

Address = tuple[str, int]
T = TypeVar("T")

#: Mensajes pendientes por enlace antes de empezar a descartar: Raft
#: tolera pérdidas, y un peer parado no debe acumular memoria sin límite.
MAX_QUEUED_MESSAGES = 1024
CONNECT_TIMEOUT_SECONDS = 0.25
SEND_TIMEOUT_SECONDS = 1.0


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(min(remaining, 1 << 16))
        if not chunk:
            raise ConnectionError("conexión cerrada")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(sock: socket.socket) -> tuple[str, Message]:
    length = frame_length(_recv_exact(sock, 4))
    return decode_payload(_recv_exact(sock, length))


class TcpTransport:
    """`Transport` asíncrono: acumula los envíos en un buzón que el runtime
    vacía tras persistir (ver docstring del módulo)."""

    def __init__(self) -> None:
        self._outbox: list[tuple[NodeId, Message]] = []

    def send_request_vote(
        self, sender: NodeId, target: NodeId, request: RequestVoteRequest
    ) -> RequestVoteResponse | None:
        self._outbox.append((target, request))
        return None

    def send_append_entries(
        self, sender: NodeId, target: NodeId, request: AppendEntriesRequest
    ) -> AppendEntriesResponse | None:
        self._outbox.append((target, request))
        return None

    def send_install_snapshot(
        self, sender: NodeId, target: NodeId, request: InstallSnapshotRequest
    ) -> InstallSnapshotResponse | None:
        self._outbox.append((target, request))
        return None

    def drain(self) -> list[tuple[NodeId, Message]]:
        out, self._outbox = self._outbox, []
        return out


class _PeerLink:
    """Conexión saliente hacia un peer: un hilo emisor que vacía la cola y
    un hilo lector por conexión que entrega las respuestas al nodo. Si la
    conexión cae, lo pendiente se descarta (pérdida de mensajes, que Raft
    tolera) y se reconecta con backoff."""

    def __init__(self, owner: NetworkNode, peer_id: NodeId, address: Address) -> None:
        self._owner = owner
        self.peer_id = peer_id
        self.address = address
        self._queue: queue.Queue[Message] = queue.Queue(maxsize=MAX_QUEUED_MESSAGES)
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"raft-link-{owner.node_id}->{peer_id}", daemon=True
        )
        self._sock: socket.socket | None = None
        self._sock_lock = threading.Lock()

    def start(self) -> None:
        self._thread.start()

    def enqueue(self, message: Message) -> None:
        try:
            self._queue.put_nowait(message)
        except queue.Full:
            pass  # pérdida deliberada: el siguiente heartbeat lo reintenta

    def stop(self) -> None:
        self._stopped.set()
        with self._sock_lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except OSError:
                    pass

    def _drain(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return

    def _run(self) -> None:
        backoff = 0.02
        while not self._stopped.is_set():
            try:
                sock = socket.create_connection(self.address, timeout=CONNECT_TIMEOUT_SECONDS)
            except OSError:
                self._drain()
                self._stopped.wait(backoff)
                backoff = min(backoff * 2, 0.2)
                continue
            backoff = 0.02
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(SEND_TIMEOUT_SECONDS)
            with self._sock_lock:
                self._sock = sock
            broken = threading.Event()
            reader = threading.Thread(target=self._read_responses, args=(sock, broken), daemon=True)
            reader.start()
            try:
                while not self._stopped.is_set() and not broken.is_set():
                    try:
                        message = self._queue.get(timeout=0.05)
                    except queue.Empty:
                        continue
                    sock.sendall(encode_frame(self._owner.node_id, message))
            except OSError:
                pass
            finally:
                broken.set()
                with self._sock_lock:
                    self._sock = None
                try:
                    sock.close()
                except OSError:
                    pass
                self._drain()

    def _read_responses(self, sock: socket.socket, broken: threading.Event) -> None:
        try:
            sock.settimeout(None)
            while not broken.is_set():
                _, message = read_frame(sock)
                self._owner.handle_response(self.peer_id, message)
        except (OSError, ConnectionError, CodecError):
            pass
        finally:
            broken.set()


class _RequestHandler(socketserver.BaseRequestHandler):
    server: _RaftServer

    def handle(self) -> None:
        sock: socket.socket = self.request
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        owner = self.server.owner
        owner.track(sock)
        try:
            while True:
                _, message = read_frame(sock)
                response = owner.handle_request(message)
                if response is not None:
                    sock.sendall(encode_frame(owner.node_id, response))
        except (OSError, ConnectionError, CodecError):
            pass
        finally:
            owner.untrack(sock)


class _RaftServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address: Address, owner: NetworkNode) -> None:
        self.owner = owner
        super().__init__(address, _RequestHandler)


class NetworkNode:
    """Un nodo Raft real: escucha en `address`, conoce a sus peers por
    configuración estática y persiste su estado en `data_dir`.

    Uso típico (un nodo por proceso)::

        node = NetworkNode("n1", ("127.0.0.1", 7001), peers, data_dir="n1")
        node.start()
        index, term = node.submit(b"comando")   # NotLeaderError si no es líder
        node.wait_for(lambda n: n.last_applied >= index, timeout=2.0)
    """

    def __init__(
        self,
        node_id: NodeId,
        address: Address,
        peers: Mapping[NodeId, Address],
        *,
        data_dir: str,
        state_machine: StateMachine | None = None,
        tick_seconds: float = 0.01,
        election_timeout_ticks: tuple[int, int] = (15, 30),
        heartbeat_interval_ticks: int = 5,
        snapshot_threshold: int | None = None,
        fsync: bool = True,
        seed: int | None = None,
    ) -> None:
        if node_id in peers:
            raise ValueError("peers no debe incluir al propio nodo")
        if not 2 <= len(peers) <= 4:
            raise ValueError("la configuración estática admite clusters de 3 a 5 nodos")
        self.node_id = node_id
        self.storage = DiskStorage(data_dir, fsync=fsync)
        persisted = self.storage.load()
        self.transport = TcpTransport()
        self.node = RaftNode(
            node_id,
            peers.keys(),
            self.transport,
            random.Random(seed),
            state_machine=state_machine,
            election_timeout_ticks=election_timeout_ticks,
            heartbeat_interval_ticks=heartbeat_interval_ticks,
            snapshot_threshold=snapshot_threshold,
        )
        restore_node(self.node, persisted)
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self._tick_seconds = tick_seconds
        self._links = {peer: _PeerLink(self, peer, addr) for peer, addr in peers.items()}
        self._server = _RaftServer(address, self)
        self._stopped = threading.Event()
        self._threads: list[threading.Thread] = []
        self._accepted: set[socket.socket] = set()
        self._accepted_lock = threading.Lock()

    @property
    def address(self) -> Address:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    # -- ciclo de vida ------------------------------------------------------

    def start(self) -> NetworkNode:
        for link in self._links.values():
            link.start()
        server = threading.Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        ticker = threading.Thread(target=self._tick_loop, name=f"raft-tick-{self.node_id}")
        ticker.daemon = True
        self._threads = [server, ticker]
        for thread in self._threads:
            thread.start()
        return self

    def stop(self) -> None:
        """Parada abrupta (equivalente a matar el proceso a efectos del
        protocolo): no se escribe nada más — todo lo relevante ya se
        persistió en el `_after()` de cada operación."""
        self._stopped.set()
        self._server.shutdown()
        self._server.server_close()
        with self._accepted_lock:
            for sock in list(self._accepted):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                except OSError:
                    pass
        for link in self._links.values():
            link.stop()
        with self.lock:
            self.storage.close()

    def track(self, sock: socket.socket) -> None:
        with self._accepted_lock:
            self._accepted.add(sock)

    def untrack(self, sock: socket.socket) -> None:
        with self._accepted_lock:
            self._accepted.discard(sock)

    # -- operación bajo el lock ------------------------------------------------

    def _after(self) -> None:
        """Persistir, luego publicar. Llamado con `self.lock` tomado."""
        if self._stopped.is_set():
            self.transport.drain()
            return
        self.storage.sync(self.node)
        for target, message in self.transport.drain():
            link = self._links.get(target)
            if link is not None:
                link.enqueue(message)
        self.changed.notify_all()

    def run(self, operation: Callable[[RaftNode], T]) -> T:
        """Ejecuta `operation(node)` con el lock y persiste/publica después."""
        with self.lock:
            try:
                return operation(self.node)
            finally:
                self._after()

    def _tick_loop(self) -> None:
        while not self._stopped.wait(self._tick_seconds):
            try:
                self.run(lambda node: node.tick())
            except Exception:  # noqa: BLE001 — un hilo de tick muerto paralizaría el nodo
                log.exception("tick de %s falló", self.node_id)

    def handle_request(self, message: Message) -> Message | None:
        def dispatch(node: RaftNode) -> Message | None:
            if isinstance(message, RequestVoteRequest):
                return node.handle_request_vote(message)
            if isinstance(message, AppendEntriesRequest):
                return node.handle_append_entries(message)
            if isinstance(message, InstallSnapshotRequest):
                return node.handle_install_snapshot(message)
            return None

        if self._stopped.is_set():
            return None
        return self.run(dispatch)

    def handle_response(self, peer_id: NodeId, message: Message) -> None:
        def dispatch(node: RaftNode) -> None:
            if isinstance(message, RequestVoteResponse):
                election.handle_request_vote_response(node, message)
            elif isinstance(message, AppendEntriesResponse):
                replication.handle_append_entries_response(node, peer_id, message)
            elif isinstance(message, InstallSnapshotResponse):
                snapshot_module.handle_install_snapshot_response(node, peer_id, message)

        if not self._stopped.is_set():
            self.run(dispatch)

    # -- API de cliente -----------------------------------------------------

    def submit(self, command: bytes) -> tuple[LogIndex, Term]:
        """Propone `command` (sólo en el líder: si no, `NotLeaderError` con
        el líder conocido) y devuelve `(índice, término)` de la entrada."""

        def propose(node: RaftNode) -> tuple[LogIndex, Term]:
            return node.propose(command), node.current_term

        return self.run(propose)

    def wait_for(self, predicate: Callable[[RaftNode], bool], timeout: float) -> bool:
        """Espera (sin consumir CPU) a que `predicate(node)` sea cierto; se
        reevalúa tras cada operación del nodo."""
        with self.changed:
            return self.changed.wait_for(lambda: predicate(self.node), timeout)

    def status(self) -> dict[str, object]:
        with self.lock:
            node = self.node
            return {
                "node_id": node.node_id,
                "role": node.role.name.lower(),
                "term": node.current_term,
                "leader_id": node.leader_id,
                "commit_index": node.commit_index,
                "last_applied": node.last_applied,
                "last_log_index": node.last_log_index(),
            }

    @property
    def is_leader(self) -> bool:
        with self.lock:
            return self.node.role is RaftRole.LEADER
