"""Testes da saída do S1 (s1/output.py): envelope comum, os três contratos
ods.s1.* e os JSON Schemas de contracts/."""

import copy
import uuid
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
import pytest

from s1.domain import ENTER, EXIT, FrameResult, Occupancy, Point, PositionItem, S1Service, Transition
from s1.ingestion import load_json, load_messages, parse_zones
from s1.output import POSITION, ZONE_OCCUPANCY, ZONE_TRANSITION, S1Publisher, format_timestamp
from s1.runtime import S1Runtime
from tests.factories import SESSION, at, calibration_message, frame, raw_track, session_message, tracks_message

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

def test_time_field_can_follow_the_kafka_guide_name():
    (message,) = S1Publisher(clock=lambda: PUBLISHED_AT, time_field="occurred_at").frame_messages(_result())
    assert "captured_at" not in message
    assert message["occurred_at"] == format_timestamp(at(500))

# --------------------------------------------------------------------------
# JSON Schemas (contracts/)
# --------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent
SCHEMAS = (POSITION, ZONE_TRANSITION, ZONE_OCCUPANCY)

def _schema(name):
    return load_json(ROOT / "contracts" / f"{name}.schema.json")

def _validator(name):
    schema = _schema(name)
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())

def _example(name, index=0):
    return copy.deepcopy(_schema(name)["examples"][index])

def _set(path, value):
    def apply(message):
        *parents, last = path
        for key in parents:
            message = message[key]
        message[last] = value
    return apply

def _drop(*path):
    def apply(message):
        *parents, last = path
        for key in parents:
            message = message[key]
        del message[last]
    return apply

# Exemplo de partida (índice em "examples" do schema) + o defeito aplicado.
# Posição: 0 = em metros. Transição: 0 = ENTER, 1 = EXIT. Ocupação: 0 = ocupada.
INVALID = {
    "position-sem-frame_index": (POSITION, 0, _drop("payload", "frame_index")),
    "position-position_m-sem-space_id": (POSITION, 0, _drop("payload", "positions", 0, "space_id")),
    "position-point_px-com-3-numeros": (POSITION, 0, _set(("payload", "positions", 0, "point_px"), [1, 2, 3])),
    "position-payload-repete-source_id": (POSITION, 0, _set(("payload", "source_id"), "cam0")),
    "position-producer-errado": (POSITION, 0, _set(("producer",), "S2")),
    "position-versao-maior-2": (POSITION, 0, _set(("schema_version",), "2.0")),
    "position-sem-captured_at": (POSITION, 0, _drop("captured_at")),
    "position-horario-sem-milissegundos": (POSITION, 0, _set(("captured_at",), "2026-09-23T14:29:59Z")),
    "transition-exit-sem-dwell_s": (ZONE_TRANSITION, 1, _drop("payload", "dwell_s")),
    "transition-enter-com-dwell_s": (ZONE_TRANSITION, 0, _set(("payload", "dwell_s"), 0)),
    "transition-dwell_s-fracionario": (ZONE_TRANSITION, 1, _set(("payload", "dwell_s"), 8395.4)),
    "transition-transition-minusculo": (ZONE_TRANSITION, 0, _set(("payload", "transition"), "enter")),
    "occupancy-count-negativo": (ZONE_OCCUPANCY, 0, _set(("payload", "count"), -1)),
    "occupancy-sem-track_ids": (ZONE_OCCUPANCY, 0, _drop("payload", "track_ids")),
    "occupancy-campo-fora-do-contrato": (ZONE_OCCUPANCY, 0, _set(("payload", "occupancy_rate"), 1.0)),
}

@pytest.mark.parametrize("name", SCHEMAS)
def test_schema_examples_are_valid(name):
    validator = _validator(name)
    assert _schema(name)["examples"]
    for example in _schema(name)["examples"]:
        validator.validate(example)

@pytest.mark.parametrize("case", INVALID, ids=list(INVALID))
def test_messages_outside_the_contract_fail_the_schema(case):
    name, index, defect = INVALID[case]
    message = _example(name, index)
    defect(message)
    assert not _validator(name).is_valid(message)

def test_everything_the_s1_publishes_matches_its_schema():
    zones = parse_zones(load_json(ROOT / "data" / "zones_a3.json"), "zones-a3-1")
    runtime = S1Runtime(S1Service(zones), S1Publisher(), zones_artifact_id="zones-a3")
    incoming = load_messages(ROOT / "data" / "i3_calibration.json") + load_messages(ROOT / "data" / "entrada_demo.json")
    # Um início de sessão no fim gera também a ocupação zerada.
    incoming.append(session_message("33333333-3333-4333-8333-333333333333", sources=("cam_frontal",)))
    published = [out for message in incoming for out in runtime.handle(message)]

    assert {message["schema"] for message in published} == set(SCHEMAS)
    validators = {name: _validator(name) for name in SCHEMAS}
    for message in published:
        validators[message["schema"]].validate(message)

def test_position_in_meters_matches_the_schema():
    zones = parse_zones(load_json(ROOT / "data" / "zones_exemplo_a8.json"), "zones-a8-2")
    runtime = S1Runtime(S1Service(zones), S1Publisher(), zones_artifact_id="zones-a8")
    runtime.handle(calibration_message(source="cam0", space_id="patio-1", scale=0.01))
    position = runtime.handle(tracks_message(0, [raw_track("t-1", 400, 900, object_class="vehicle")]))[0]
    assert "position_m" in position["payload"]["positions"][0]
    _validator(POSITION).validate(position)
