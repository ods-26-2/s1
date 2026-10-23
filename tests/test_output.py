"""Testes da saída do S1 (s1/output.py): envelope comum e os três
contratos ods.s1.*."""

import uuid
from datetime import datetime, timezone

from s1.domain import ENTER, EXIT, FrameResult, Occupancy, Point, PositionItem, Transition
from s1.output import S1Publisher, format_timestamp
from tests.factories import SESSION, at, frame

PUBLISHED_AT = datetime(2026, 9, 23, 14, 30, 0, 120_000, tzinfo=timezone.utc)
ENVELOPE_FIELDS = [
    "message_type", "message_id", "schema", "schema_version", "producer", "correlation_id", "published_at",
    "captured_at", "source_id", "session_id", "sequence", "artifact_version", "payload",
]

def _publisher():
    return S1Publisher(clock=lambda: PUBLISHED_AT)

def _position(position_m=None):
    return PositionItem(
        track_id="t-0391", object_class="person", point_px=Point(505, 702),
        position_m=position_m, space_id="patio-1" if position_m else None, zone_ids=("vaga-12",),
    )

def _occupancy(count=0, max_occupancy=1):
    return Occupancy(
        source_id="cam0", session_id=SESSION, artifact_version="zones-a8-2", zone_id="vaga-12", count=count,
        count_by_class={"vehicle": count}, track_ids=tuple(f"t-{i}" for i in range(count)), at=at(500),
        max_occupancy=max_occupancy,
    )

def _result(positions=(), transitions=(), occupancy=()):
    return FrameResult(frame(5, message_id="i2-msg"), "zones-a8-2", tuple(positions), tuple(transitions), tuple(occupancy))

def test_envelope_has_the_13_fields_of_the_ev_sinal_profile():
    (message,) = _publisher().frame_messages(_result())

    assert list(message) == ENVELOPE_FIELDS
    assert uuid.UUID(message["message_id"]).version == 4
    assert (message["message_type"], message["schema"], message["schema_version"], message["producer"]) == (
        "event", "ods.s1.position", "1.0", "S1",
    )
    assert message["published_at"] == "2026-09-23T14:30:00.120Z"
    assert message["captured_at"] == format_timestamp(at(500))
    assert (message["source_id"], message["session_id"], message["artifact_version"]) == ("cam0", SESSION, "zones-a8-2")
    assert message["correlation_id"] == "i2-msg"

def test_sequence_counts_per_session_from_zero():
    publisher = _publisher()
    first = publisher.frame_messages(_result(occupancy=[_occupancy()]))
    second = publisher.frame_messages(_result())
    other_session = publisher.occupancy_messages([Occupancy("cam0", "outra", "v", "z", 0, {}, (), at(0))])

    assert [m["sequence"] for m in first + second] == [0, 1, 2]
    assert other_session[0]["sequence"] == 0

def test_position_payload_matches_the_contract():
    publisher = _publisher()
    (meters,) = publisher.frame_messages(_result([_position(Point(3.42, 7.1))]))
    (pixels,) = publisher.frame_messages(_result([_position()]))

    assert meters["payload"] == {
        "frame_index": 5,
        "positions": [
            {"track_id": "t-0391", "class": "person", "point_px": [505, 702], "position_m": [3.42, 7.1],
             "space_id": "patio-1", "zone_ids": ["vaga-12"]}
        ],
    }
    assert "position_m" not in pixels["payload"]["positions"][0]
    assert "space_id" not in pixels["payload"]["positions"][0]

def test_empty_frame_still_publishes_position():
    (message,) = _publisher().frame_messages(_result())
    assert message["payload"] == {"frame_index": 5, "positions": []}

def test_transition_payload_and_crossing_instant():
    enter = Transition("cam0", "t-0391", "vehicle", "vaga-12", ENTER, at(100), at(100))
    exit_ = Transition("cam0", "t-0391", "vehicle", "vaga-12", EXIT, at(8_395_100), at(100), 8395)
    _, enter_message, exit_message = _publisher().frame_messages(_result(transitions=[enter, exit_]))

    assert enter_message["schema"] == "ods.s1.zone_transition"
    assert enter_message["captured_at"] == format_timestamp(at(100))
    assert enter_message["payload"] == {
        "track_id": "t-0391", "class": "vehicle", "zone_id": "vaga-12", "transition": "ENTER",
        "entered_at": format_timestamp(at(100)),
    }
    assert exit_message["captured_at"] == format_timestamp(at(8_395_100))
    assert exit_message["payload"]["transition"] == "EXIT"
    assert exit_message["payload"]["dwell_s"] == 8395

def test_occupancy_payload_matches_the_contract():
    _, message = _publisher().frame_messages(_result(occupancy=[_occupancy(count=1)]))

    assert message["schema"] == "ods.s1.zone_occupancy"
    assert message["payload"] == {
        "zone_id": "vaga-12", "count": 1, "count_by_class": {"vehicle": 1}, "max_occupancy": 1, "track_ids": ["t-0"],
    }

def test_occupancy_omits_max_occupancy_when_zone_has_none():
    (message,) = _publisher().occupancy_messages([_occupancy(max_occupancy=None)])
    assert "max_occupancy" not in message["payload"]

def test_payload_never_repeats_envelope_fields():
    transition = Transition("cam0", "t-1", "person", "z", EXIT, at(0), at(0), 1)
    messages = _publisher().frame_messages(_result([_position(Point(1, 2))], [transition], [_occupancy(count=1)]))
    for message in messages:
        assert not set(message["payload"]) & set(ENVELOPE_FIELDS)
