"""Testes de s1/__main__.py: configuração por variáveis de ambiente e
montagem do S1 na partida."""

import hashlib
import json
from pathlib import Path

import pytest

from s1.__main__ import ConfigError, S1Config, build_runtime, create_clients, kafka_key, load_pinned_zones
from s1.ingestion import ContractError, default_artifact_id
from s1.runtime import Topics
from tests.factories import raw_track, tracks_message

DATA_DIR = Path(__file__).parent.parent / "data"


def _env(**overrides):
    env = {
        "S1_ZONES_FILE": str(DATA_DIR / "zones_a3.json"),
        "S1_ZONES_VERSION": "zones-a3-1",
        "S1_KAFKA_BOOTSTRAP_SERVERS": "broker.exemplo:9093",
    }
    env.update(overrides)
    return {key: value for key, value in env.items() if value is not None}

def test_minimal_configuration_and_defaults():
    config = S1Config.from_env(_env())

    assert config.zones_artifact_id == "zones-a3"
    assert config.kafka == {
        "bootstrap.servers": "broker.exemplo:9093",
        "group.id": "S1-zonas-zones-a3",
        "auto.offset.reset": "latest",
        "client.id": "S1-zonas-zones-a3",
    }
    assert config.topics == Topics()
    assert (config.time_field, config.registry_dir, config.calibration_file) == ("captured_at", None, None)

def test_each_application_gets_its_own_consumer_group_by_default():
    a3 = S1Config.from_env(_env())
    a8 = S1Config.from_env(_env(S1_ZONES_VERSION="zones-a8-2"))
    assert a3.kafka["group.id"] != a8.kafka["group.id"]

def test_any_librdkafka_key_can_be_passed_by_environment():
    config = S1Config.from_env(_env(
        S1_KAFKA_GROUP_ID="S1-zonas-a8-cam0",
        S1_KAFKA_SECURITY_PROTOCOL="SASL_SSL",
        S1_KAFKA_SASL_MECHANISMS="PLAIN",
        S1_KAFKA_AUTO_OFFSET_RESET="earliest",
    ))
    assert config.kafka["group.id"] == "S1-zonas-a8-cam0"
    assert config.kafka["security.protocol"] == "SASL_SSL"
    assert config.kafka["sasl.mechanisms"] == "PLAIN"
    assert config.kafka["auto.offset.reset"] == "earliest"

@pytest.mark.parametrize(
    "suffix, key",
    [("BOOTSTRAP_SERVERS", "bootstrap.servers"), ("SASL_USERNAME", "sasl.username"), ("LOG__LEVEL", "log_level")],
)
def test_environment_suffix_becomes_a_librdkafka_key(suffix, key):
    assert kafka_key(suffix) == key

def test_producer_does_not_receive_consumer_only_keys():
    config = S1Config.from_env(_env(S1_KAFKA_SECURITY_PROTOCOL="SASL_SSL"))
    assert "group.id" not in config.producer_config and "auto.offset.reset" not in config.producer_config
    assert config.producer_config["security.protocol"] == "SASL_SSL"
    assert config.consumer_config["group.id"] == "S1-zonas-zones-a3"

def test_topics_can_be_renamed_by_environment():
    config = S1Config.from_env(_env(S1_TOPIC_I2_TRACKS="ods.visao.rastros", S1_TOPIC_ZONE_TRANSITION="ods.espaco.transicao"))
    assert config.topics.inputs()[0] == "ods.visao.rastros"
    assert config.topics.output_for("ods.s1.zone_transition") == "ods.espaco.transicao"
    assert config.topics.output_for("ods.s1.position") == "ods.s1.position"

@pytest.mark.parametrize(
    "overrides",
    [
        {"S1_KAFKA_BOOTSTRAP_SERVERS": None},
        {"S1_KAFKA_BOOTSTRAP_SERVERS": "  "},
        {"S1_ZONES_FILE": None},
        {"S1_ZONES_VERSION": None},
        {"S1_TIME_FIELD": "timestamp"},
    ],
    ids=["sem-broker", "broker-vazio", "sem-zones", "sem-versao", "campo-de-tempo-invalido"],
)
def test_missing_or_invalid_configuration_is_rejected(overrides):
    with pytest.raises(ConfigError):
        S1Config.from_env(_env(**overrides))

def test_default_artifact_id_drops_the_version_number():
    assert default_artifact_id("zones-a8-2") == "zones-a8"
    assert default_artifact_id("zones-a3-12") == "zones-a3"
    assert default_artifact_id("zonas") == "zonas"

def test_pinned_zones_are_checked_against_the_configured_hash():
    data = (DATA_DIR / "zones_a3.json").read_bytes()
    good = S1Config.from_env(_env(S1_ZONES_SHA256=hashlib.sha256(data).hexdigest()))
    bad = S1Config.from_env(_env(S1_ZONES_SHA256="0" * 64))

    assert load_pinned_zones(good).artifact_version == "zones-a3-1"
    with pytest.raises(ContractError):
        load_pinned_zones(bad)

def test_build_runtime_applies_startup_calibration_and_time_field(tmp_path):
    calibration = json.loads((DATA_DIR / "i3_calibration.json").read_text())
    config = S1Config.from_env(_env(
        S1_CALIBRATION_FILE=str(DATA_DIR / "i3_calibration.json"),
        S1_REGISTRY_DIR=str(tmp_path),
        S1_TIME_FIELD="occurred_at",
    ))
    runtime = build_runtime(config)
    (position,) = runtime.handle(tracks_message(0, [raw_track("t-1", 800, 900)], source="cam_frontal"))[:1]

    assert "position_m" in position["payload"]["positions"][0]
    assert position["payload"]["positions"][0]["space_id"] == calibration[0]["payload"]["reference_frame"]["space_id"]
    assert "occurred_at" in position and "captured_at" not in position

def test_real_confluent_kafka_clients_accept_the_generated_configuration():
    """Só cria os clientes (não conecta): o librdkafka rejeita na criação
    qualquer chave de configuração que não exista."""

    config = S1Config.from_env({
        "S1_ZONES_FILE": "data/zones_a3.json",
        "S1_ZONES_VERSION": "zones-a3-1",
        "S1_KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
        "S1_KAFKA_SOCKET_TIMEOUT_MS": "1000",
    })
    consumer, producer = create_clients(config)
    consumer.close()
    assert producer.flush(0) == 0
