"""Testes de s1/runtime.py: a ponte barramento → S1 (incluindo o cenário
completo da demonstração em data/) e o adaptador Kafka, com consumidor e
produtor falsos que imitam o confluent-kafka — nenhum broker é usado."""

import hashlib
import json
import logging
from pathlib import Path
import pytest

from s1.domain import S1Service
from s1.ingestion import ArtifactRegistry, load_json, load_messages, parse_zones
from s1.output import S1Publisher
from s1.runtime import KafkaBus, S1Runtime, Topics, encode_message
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
        "schema": "ods.artifact.published", "schema_version": "1.0", "message_type": "event", "message_id": "a-1", "producer": "A3",
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

# --------------------------------------------------------------------------
# Cenário da demonstração (data/zones_a3.json + data/entrada_demo.json)
# --------------------------------------------------------------------------

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
        "schema": "ods.artifact.published", "schema_version": "1.0", "message_type": "event", "message_id": "a-2", "producer": "A3",
        "payload": {"artifact_id": "zones-t", "artifact_version": "zones-t-2", "kind": "ZONES", "owner": "A3",
                    "uri": "registry://z.json", "hash_sha256": hashlib.sha256(data).hexdigest(),
                    "valid_from": "2026-09-23T14:00:00.000Z", "schema_version": "1.0"},
    })
    out = runtime.handle(tracks_message(0, [raw_track("t-1", 50, 50)]))
    assert {m["artifact_version"] for m in out} == {"zones-t-2"}
    assert out[0]["payload"]["positions"][0]["zone_ids"] == ["z2"]

def test_new_camera_mid_session_does_not_apply_scheduled_zones():
    def zones(version):
        return artifact(square("z", 0, 0, 100, 100, 0, 0), coordinates="METERS", space_id="patio-1",
                        source_ids=("cam0", "cam1"), version=version)

    service = S1Service(zones("zones-t-1"))
    runtime = S1Runtime(service, S1Publisher(), zones_artifact_id="zones-t")
    runtime.handle(calibration_message(source="cam0", space_id="patio-1", scale=1))
    runtime.handle(tracks_message(0, [raw_track("t-1", 50, 50)], source="cam0"))
    service.schedule_zones(zones("zones-t-2"))

    out = runtime.handle(tracks_message(0, [], source="cam1"))
    assert out[0]["artifact_version"] == "zones-t-1"
    assert service.occupancy_snapshot("cam0") == {"z": 1}

# --------------------------------------------------------------------------
# Adaptador Kafka
# --------------------------------------------------------------------------

class FakeError:
    def __init__(self, text="broker indisponível", fatal=False):
        self._text, self._fatal = text, fatal

    def fatal(self):
        return self._fatal

    def __str__(self):
        return self._text

class FakeRecord:
    def __init__(self, value=None, topic="ods.i2.tracks", error=None, offset=0):
        self._value, self._topic, self._error, self._offset = value, topic, error, offset

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def error(self):
        return self._error

    def offset(self):
        return self._offset

class FakeConsumer:
    def __init__(self, records=()):
        self.records = list(records)
        self.subscribed = None
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def poll(self, timeout):
        return self.records.pop(0) if self.records else None

    def close(self):
        self.closed = True

class FakeProducer:
    def __init__(self, buffer_full_times=0, fail_delivery=False):
        self.produced = []
        self.flushed = False
        self._buffer_full_times = buffer_full_times
        self._fail_delivery = fail_delivery

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self._buffer_full_times:
            self._buffer_full_times -= 1
            raise BufferError("fila local cheia")
        self.produced.append((topic, key, value))
        if on_delivery:
            on_delivery(FakeError("timeout") if self._fail_delivery else None, FakeRecord(value, topic))

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        self.flushed = True
        return 0

def _bus(records=(), topics=Topics(), producer=None, runtime=None):
    runtime = runtime or S1Runtime(
        S1Service(artifact(square("z", 0, 0, 100, 100, 0, 0), version="zones-t-1")),
        S1Publisher(),
        zones_artifact_id="zones-t",
    )
    consumer, producer = FakeConsumer(records), producer or FakeProducer()
    return KafkaBus(runtime, consumer, producer, topics, poll_timeout_s=0), consumer, producer

def _record(message, **kwargs):
    return FakeRecord(encode_message(message), **kwargs)

def test_subscribes_to_the_four_input_topics():
    bus, consumer, _ = _bus()
    bus.start()
    assert consumer.subscribed == ["ods.i2.tracks", "ods.i3.calibration", "ods.session.started", "ods.artifact.published"]

def test_frame_is_published_on_the_schema_topics_keyed_by_source_id():
    bus, _, producer = _bus([_record(tracks_message(0, [raw_track("t-1", 50, 50)]))])

    assert bus.poll_once() == 3
    assert [(topic, key) for topic, key, _ in producer.produced] == [
        ("ods.s1.position", b"cam0"),
        ("ods.s1.zone_transition", b"cam0"),
        ("ods.s1.zone_occupancy", b"cam0"),
    ]
    position = json.loads(producer.produced[0][2])
    assert position["producer"] == "S1" and position["payload"]["positions"][0]["zone_ids"] == ["z"]

def test_topic_names_come_from_configuration():
    topics = Topics(i2_tracks="ods.visao.rastros", position="ods.espaco.posicao")
    bus, consumer, producer = _bus([_record(tracks_message(0, []), topic="ods.visao.rastros")], topics=topics)
    bus.start()
    bus.poll_once()

    assert consumer.subscribed[0] == "ods.visao.rastros"
    assert producer.produced[0][0] == "ods.espaco.posicao"

@pytest.mark.parametrize(
    "record",
    [
        FakeRecord(b"{isto nao e json"),
        FakeRecord(b"[1, 2, 3]"),
        FakeRecord(encode_message({**tracks_message(0, []), "schema_version": "2.0"})),
        FakeRecord(error=FakeError()),
        None,
    ],
    ids=["json-invalido", "json-nao-objeto", "versao-maior-2", "erro-do-kafka", "sem-mensagem"],
)
def test_bad_or_missing_records_are_dropped_and_the_next_is_processed(record):
    bus, _, producer = _bus([record, _record(tracks_message(1, []))])

    assert bus.poll_once() == 0
    assert bus.poll_once() == 1
    assert [topic for topic, _, _ in producer.produced] == ["ods.s1.position"]

def test_fatal_kafka_error_stops_the_service():
    bus, _, _ = _bus([FakeRecord(error=FakeError("credenciais inválidas", fatal=True))])
    with pytest.raises(RuntimeError):
        bus.poll_once()

def test_unexpected_failure_in_one_message_does_not_stop_the_service(caplog):
    class Exploding:
        def handle(self, message):
            raise KeyError("defeito")

    bus, _, _ = _bus([_record(tracks_message(0, []))], runtime=Exploding())
    with caplog.at_level(logging.ERROR):
        assert bus.poll_once() == 0
    assert "falha inesperada" in caplog.text

def test_full_local_queue_is_retried_once():
    bus, _, producer = _bus([_record(tracks_message(0, []))], producer=FakeProducer(buffer_full_times=1))
    bus.poll_once()
    assert len(producer.produced) == 1

def test_delivery_failures_are_counted_and_logged(caplog):
    bus, _, _ = _bus([_record(tracks_message(0, []))], producer=FakeProducer(fail_delivery=True))
    with caplog.at_level(logging.ERROR):
        bus.poll_once()
    assert bus.delivery_failures == 1 and "falha ao publicar" in caplog.text

def test_run_processes_until_stopped_then_flushes_and_closes():
    records = [_record(tracks_message(index, [])) for index in range(3)]
    bus, consumer, producer = _bus(records)

    bus.run(should_stop=lambda: not consumer.records)

    assert len(producer.produced) == 3
    assert producer.flushed and consumer.closed

def test_duplicate_delivery_from_the_broker_is_published_once():
    message = tracks_message(0, [])
    bus, _, producer = _bus([_record(message), _record(message)])
    bus.poll_once()
    bus.poll_once()
    assert len(producer.produced) == 1
