"""Testes da ponte barramento → S1 (s1/runtime.py), incluindo o cenário
completo da demonstração em data/."""

import hashlib
import json
from pathlib import Path

from s1.domain import S1Service
from s1.ingestion import ArtifactRegistry, load_json, load_messages, parse_zones
from s1.output import S1Publisher
from s1.runtime import S1Runtime
from tests.factories import artifact, calibration_message, raw_track, session_message, square, tracks_message

DATA_DIR = Path(__file__).parent.parent / "data"
NEW_SESSION = "22222222-2222-4222-8222-222222222222"

def _runtime(zones=None, registry=None):
    zones = zones or artifact(square("z", 0, 0, 100, 100, 0, 0), version="zones-t-1")
    return S1Runtime(S1Service(zones), S1Publisher(), zones_artifact_id="zones-t", registry=registry)

def _schemas(messages):
    return [message["schema"] for message in messages]

def test_track_frame_publishes_position_transition_and_occupancy():
    out = _runtime().handle(tracks_message(0, [raw_track("t-1", 50, 50)]))
    assert _schemas(out) == ["ods.s1.position", "ods.s1.zone_transition", "ods.s1.zone_occupancy"]

def test_duplicate_message_id_is_processed_once():
    runtime = _runtime()
    message = tracks_message(0, [raw_track("t-1", 50, 50)])
    assert runtime.handle(message) != []
    assert runtime.handle(dict(message)) == []

def test_message_outside_the_contract_is_dropped_without_stopping_the_service():
    runtime = _runtime()
    broken = tracks_message(0, [raw_track("t-1", 50, 50, state="TENTATIVE")])
    assert runtime.handle(broken) == []
    assert runtime.handle(tracks_message(1, [raw_track("t-1", 50, 50)])) != []

def test_other_schemas_are_ignored():
    assert _runtime().handle({"schema": "ods.i1.detections", "message_id": "x", "payload": {}}) == []

def test_calibration_message_enables_position_in_meters():
    zones = artifact(square("vaga-12", 3, 6.5, 5.5, 11.5, 0, 0), coordinates="METERS", space_id="patio-1")
    runtime = _runtime(zones)
    runtime.handle(calibration_message(source="cam0", space_id="patio-1", scale=0.01))
    position = runtime.handle(tracks_message(0, [raw_track("t-1", 400, 900)]))[0]

    assert position["payload"]["positions"][0]["position_m"] == [4.0, 9.0]
    assert position["payload"]["positions"][0]["zone_ids"] == ["vaga-12"]

def test_session_started_zeroes_occupancy_in_the_new_session():
    runtime = _runtime()
    runtime.handle(tracks_message(0, [raw_track("t-1", 50, 50)]))
    (message,) = runtime.handle(session_message(NEW_SESSION))

    assert message["schema"] == "ods.s1.zone_occupancy"
    assert (message["session_id"], message["sequence"], message["payload"]["count"]) == (NEW_SESSION, 0, 0)

def test_zones_announced_mid_session_are_loaded_from_the_registry_and_apply_at_next_session(tmp_path):
    content = {"coordinates": "PIXELS", "source_ids": ["cam0"], "zones": [
        {"zone_id": "z2", "name": "Z2", "polygon": [[0, 0], [100, 0], [100, 100]],
         "enter_hysteresis_ms": 0, "exit_hysteresis_ms": 0}]}
    data = json.dumps(content).encode()
    (tmp_path / "zones-t-2.json").write_bytes(data)
    runtime = _runtime(registry=ArtifactRegistry(tmp_path))
    runtime.handle(tracks_message(0, [raw_track("t-1", 50, 50)]))  # sessão em andamento

    runtime.handle({
        "schema": "ods.artifact.published", "message_type": "event", "message_id": "a-1", "producer": "A3",
        "payload": {"artifact_id": "zones-t", "artifact_version": "zones-t-2", "kind": "ZONES", "owner": "A3",
                    "uri": "registry://zones-t-2.json", "hash_sha256": hashlib.sha256(data).hexdigest(),
                    "valid_from": "2026-09-23T14:00:00.000Z", "schema_version": "1.0"},
    })
    before = runtime.handle(tracks_message(1, [raw_track("t-1", 50, 50)]))
    reset = runtime.handle(session_message(NEW_SESSION))
    after = runtime.handle(tracks_message(0, [], session=NEW_SESSION))

    assert before[0]["artifact_version"] == "zones-t-1"
    # A zeragem é das zonas antigas, então sai com a versão antiga.
    assert [(m["payload"]["zone_id"], m["artifact_version"]) for m in reset] == [("z", "zones-t-1")]
    assert after[0]["artifact_version"] == "zones-t-2"

def _demo():
    zones = parse_zones(load_json(DATA_DIR / "zones_a3.json"), "zones-a3-1")
    runtime = S1Runtime(S1Service(zones), S1Publisher(), zones_artifact_id="zones-a3")
    return [out for message in load_messages(DATA_DIR / "entrada_demo.json") for out in runtime.handle(message)]

def test_demo_publishes_one_position_per_frame():
    positions = [m for m in _demo() if m["schema"] == "ods.s1.position"]
    assert [m["payload"]["frame_index"] for m in positions] == [0, 1, 2, 3, 4, 5, 40, 41]

def test_demo_object_dancing_on_border_never_transitions():
    transitions = [m for m in _demo() if m["schema"] == "ods.s1.zone_transition"]
    assert all(m["payload"]["track_id"] != "t-0001" for m in transitions)

def test_demo_object_99_enters_and_exits_when_lost():
    transitions = [m["payload"] for m in _demo() if m["schema"] == "ods.s1.zone_transition"]
    occupancy = [m["payload"] for m in _demo() if m["schema"] == "ods.s1.zone_occupancy"]

    assert [(t["track_id"], t["transition"], t["zone_id"]) for t in transitions] == [
        ("t-0099", "ENTER", "zona_segura_01"),
        ("t-0099", "EXIT", "zona_segura_01"),
    ]
    assert transitions[0]["entered_at"] == "2026-09-16T10:47:28.400Z"
    assert transitions[1]["dwell_s"] == 4
    assert [(o["count"], o["track_ids"]) for o in occupancy] == [(1, ["t-0099"]), (0, [])]

def test_demo_messages_carry_zones_version_and_increasing_sequence():
    messages = _demo()
    assert {m["artifact_version"] for m in messages} == {"zones-a3-1"}
    assert [m["sequence"] for m in messages] == list(range(len(messages)))

def test_zones_announced_before_any_frame_apply_right_away(tmp_path):
    content = {"coordinates": "PIXELS", "source_ids": ["cam0"], "zones": [
        {"zone_id": "z2", "name": "Z2", "polygon": [[0, 0], [100, 0], [100, 100], [0, 100]],
         "enter_hysteresis_ms": 0, "exit_hysteresis_ms": 0}]}
    data = json.dumps(content).encode()
    (tmp_path / "z.json").write_bytes(data)
    runtime = _runtime(registry=ArtifactRegistry(tmp_path))
    runtime.handle({
        "schema": "ods.artifact.published", "message_type": "event", "message_id": "a-2", "producer": "A3",
        "payload": {"artifact_id": "zones-t", "artifact_version": "zones-t-2", "kind": "ZONES", "owner": "A3",
                    "uri": "registry://z.json", "hash_sha256": hashlib.sha256(data).hexdigest(),
                    "valid_from": "2026-09-23T14:00:00.000Z", "schema_version": "1.0"},
    })
    out = runtime.handle(tracks_message(0, [raw_track("t-1", 50, 50)]))
    assert {m["artifact_version"] for m in out} == {"zones-t-2"}
    assert out[0]["payload"]["positions"][0]["zone_ids"] == ["z2"]

def test_new_camera_mid_session_does_not_apply_scheduled_zones():
    service = S1Service(artifact(square("z", 0, 0, 100, 100, 0, 0), source_ids=("cam0",), version="zones-t-1"))
    runtime = S1Runtime(service, S1Publisher(), zones_artifact_id="zones-t")
    runtime.handle(tracks_message(0, [raw_track("t-1", 50, 50)], source="cam0"))
    service.schedule_zones(artifact(square("z", 0, 0, 100, 100, 0, 0), version="zones-t-2"))

    out = runtime.handle(tracks_message(0, [], source="cam1"))
    assert out[0]["artifact_version"] == "zones-t-1"
    assert service.occupancy_snapshot("cam0") == {"z": 1}
