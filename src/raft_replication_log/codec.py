"""Serialización de los RPC de Raft para un transporte de red real.

Un mensaje viaja como un marco `[u32 longitud][JSON UTF-8]`. El JSON es un
sobre `{"kind": ..., "sender": ..., "body": {...}}`; los `bytes` opacos
(comandos de log, estado de snapshot) se codifican en base64. JSON en vez
de un formato binario propio: los mensajes son pequeños, legibles al
depurar una partición con `tcpdump`, y la decodificación valida tipos en
la frontera — un marco corrupto o de una versión distinta se rechaza con
`CodecError`, nunca se convierte en un `LogEntry` a medias.
"""

from __future__ import annotations

import base64
import json
import struct
from typing import Any

from .errors import RaftError
from .models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
    Snapshot,
)

#: Tope de tamaño de un marco: un snapshot grande cabe de sobra, una
#: longitud corrupta (gigabytes) se rechaza antes de reservar memoria.
MAX_FRAME_BYTES = 256 * 1024 * 1024

Message = (
    RequestVoteRequest
    | RequestVoteResponse
    | AppendEntriesRequest
    | AppendEntriesResponse
    | InstallSnapshotRequest
    | InstallSnapshotResponse
)


class CodecError(RaftError):
    """Marco o mensaje mal formado."""


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(text: object) -> bytes:
    if not isinstance(text, str):
        raise CodecError("se esperaba una cadena base64")
    try:
        return base64.b64decode(text, validate=True)
    except ValueError as exc:
        raise CodecError("base64 inválido") from exc


def _entry_to_json(entry: LogEntry) -> dict[str, Any]:
    return {"index": entry.index, "term": entry.term, "command": _b64(entry.command)}


def _entry_from_json(raw: dict[str, Any]) -> LogEntry:
    return LogEntry(index=int(raw["index"]), term=int(raw["term"]), command=_unb64(raw["command"]))


def message_to_json(message: Message) -> tuple[str, dict[str, Any]]:
    if isinstance(message, RequestVoteRequest):
        return "request_vote", {
            "term": message.term,
            "candidate_id": message.candidate_id,
            "last_log_index": message.last_log_index,
            "last_log_term": message.last_log_term,
        }
    if isinstance(message, RequestVoteResponse):
        return "request_vote_response", {
            "term": message.term,
            "vote_granted": message.vote_granted,
            "voter_id": message.voter_id,
        }
    if isinstance(message, AppendEntriesRequest):
        return "append_entries", {
            "term": message.term,
            "leader_id": message.leader_id,
            "prev_log_index": message.prev_log_index,
            "prev_log_term": message.prev_log_term,
            "entries": [_entry_to_json(e) for e in message.entries],
            "leader_commit": message.leader_commit,
        }
    if isinstance(message, AppendEntriesResponse):
        return "append_entries_response", {
            "term": message.term,
            "success": message.success,
            "responder_id": message.responder_id,
            "match_index": message.match_index,
            "conflict_index": message.conflict_index,
            "conflict_term": message.conflict_term,
        }
    if isinstance(message, InstallSnapshotRequest):
        snap = message.snapshot
        return "install_snapshot", {
            "term": message.term,
            "leader_id": message.leader_id,
            "last_included_index": snap.last_included_index,
            "last_included_term": snap.last_included_term,
            "state": _b64(snap.state),
        }
    return "install_snapshot_response", {
        "term": message.term,
        "responder_id": message.responder_id,
    }


def message_from_json(kind: str, body: dict[str, Any]) -> Message:
    try:
        if kind == "request_vote":
            return RequestVoteRequest(
                term=int(body["term"]),
                candidate_id=str(body["candidate_id"]),
                last_log_index=int(body["last_log_index"]),
                last_log_term=int(body["last_log_term"]),
            )
        if kind == "request_vote_response":
            return RequestVoteResponse(
                term=int(body["term"]),
                vote_granted=bool(body["vote_granted"]),
                voter_id=str(body["voter_id"]),
            )
        if kind == "append_entries":
            return AppendEntriesRequest(
                term=int(body["term"]),
                leader_id=str(body["leader_id"]),
                prev_log_index=int(body["prev_log_index"]),
                prev_log_term=int(body["prev_log_term"]),
                entries=tuple(_entry_from_json(e) for e in body["entries"]),
                leader_commit=int(body["leader_commit"]),
            )
        if kind == "append_entries_response":
            conflict_index = body.get("conflict_index")
            conflict_term = body.get("conflict_term")
            return AppendEntriesResponse(
                term=int(body["term"]),
                success=bool(body["success"]),
                responder_id=str(body["responder_id"]),
                match_index=int(body.get("match_index", 0)),
                conflict_index=None if conflict_index is None else int(conflict_index),
                conflict_term=None if conflict_term is None else int(conflict_term),
            )
        if kind == "install_snapshot":
            return InstallSnapshotRequest(
                term=int(body["term"]),
                leader_id=str(body["leader_id"]),
                snapshot=Snapshot(
                    last_included_index=int(body["last_included_index"]),
                    last_included_term=int(body["last_included_term"]),
                    state=_unb64(body["state"]),
                ),
            )
        if kind == "install_snapshot_response":
            return InstallSnapshotResponse(
                term=int(body["term"]), responder_id=str(body["responder_id"])
            )
    except (KeyError, TypeError, ValueError) as exc:
        raise CodecError(f"cuerpo de mensaje '{kind}' inválido: {exc}") from exc
    raise CodecError(f"tipo de mensaje desconocido: {kind!r}")


def encode_frame(sender: str, message: Message) -> bytes:
    kind, body = message_to_json(message)
    payload = json.dumps(
        {"kind": kind, "sender": sender, "body": body}, separators=(",", ":")
    ).encode("utf-8")
    return struct.pack("!I", len(payload)) + payload


def decode_payload(payload: bytes) -> tuple[str, Message]:
    """Decodifica el cuerpo de un marco (sin la cabecera de longitud)."""
    try:
        envelope = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodecError("marco que no es JSON UTF-8 válido") from exc
    if not isinstance(envelope, dict) or not isinstance(envelope.get("body"), dict):
        raise CodecError("sobre de mensaje inválido")
    return str(envelope.get("sender", "")), message_from_json(
        str(envelope.get("kind")), envelope["body"]
    )


def frame_length(header: bytes) -> int:
    (length,) = struct.unpack("!I", header)
    if length > MAX_FRAME_BYTES:
        raise CodecError(f"marco de {length} bytes supera el máximo {MAX_FRAME_BYTES}")
    return int(length)
