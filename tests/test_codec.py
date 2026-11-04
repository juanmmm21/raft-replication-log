"""Codec de red: ida y vuelta exacta de los seis RPC y rechazo tipado de
marcos corruptos."""

from __future__ import annotations

import random

import pytest

from raft_replication_log.codec import (
    CodecError,
    decode_payload,
    encode_frame,
    frame_length,
)
from raft_replication_log.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    InstallSnapshotResponse,
    LogEntry,
    RequestVoteRequest,
    RequestVoteResponse,
    Snapshot,
)


def _roundtrip(message: object) -> object:
    frame = encode_frame("n1", message)  # type: ignore[arg-type]
    assert frame_length(frame[:4]) == len(frame) - 4
    sender, decoded = decode_payload(frame[4:])
    assert sender == "n1"
    return decoded


def test_every_rpc_round_trips() -> None:
    entries = (
        LogEntry(1, 1, b""),
        LogEntry(2, 1, b"\x00\xffbinario"),
        LogEntry(3, 2, "ñ".encode()),
    )
    messages = [
        RequestVoteRequest(term=3, candidate_id="n2", last_log_index=7, last_log_term=2),
        RequestVoteResponse(term=3, vote_granted=True, voter_id="n3"),
        AppendEntriesRequest(
            term=2,
            leader_id="n1",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=1,
        ),
        AppendEntriesResponse(term=2, success=True, responder_id="n2", match_index=3),
        AppendEntriesResponse(
            term=2, success=False, responder_id="n2", conflict_index=4, conflict_term=None
        ),
        InstallSnapshotRequest(
            term=5, leader_id="n1", snapshot=Snapshot(10, 4, bytes(range(256)) * 4)
        ),
        InstallSnapshotResponse(term=5, responder_id="n3"),
    ]
    for message in messages:
        assert _roundtrip(message) == message


def test_random_entries_round_trip() -> None:
    rng = random.Random(1)
    for _ in range(200):
        count = rng.randint(0, 8)
        entries = tuple(
            LogEntry(i + 1, rng.randint(1, 5), rng.randbytes(rng.randint(0, 64)))
            for i in range(count)
        )
        message = AppendEntriesRequest(
            term=rng.randint(1, 9),
            leader_id=f"n{rng.randint(1, 5)}",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=rng.randint(0, count),
        )
        assert _roundtrip(message) == message


def test_corrupt_frames_raise_codec_error() -> None:
    with pytest.raises(CodecError):
        decode_payload(b"\xff\xfe not json")
    with pytest.raises(CodecError):
        decode_payload(b'{"kind": "append_entries", "sender": "x", "body": {"term": 1}}')
    with pytest.raises(CodecError):
        decode_payload(b'{"kind": "nope", "sender": "x", "body": {}}')
    with pytest.raises(CodecError):
        frame_length(b"\xff\xff\xff\xff")
