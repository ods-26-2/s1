"""Ponto de entrada do S1 em produção: `python -m s1`.

Lê a configuração das variáveis de ambiente, carrega o ZONES fixado na build
(conferindo o hash, se informado) e as calibrações iniciais, e fica no laço
do barramento até receber SIGINT ou SIGTERM (o `docker stop` manda SIGTERM).

Nada de endereço de broker, grupo de consumo ou versão de artefato fica
fixo no código: o broker da nuvem é temporário (guia do Kafka, TRA-1) e cada
build fixa a sua versão do ZONES.

| Variável | Obrigatória | Para quê |
|---|---|---|
| `S1_ZONES_FILE` | sim | conteúdo do ZONES fixado na build |
| `S1_ZONES_VERSION` | sim | `artifact_version` desse ZONES |
| `S1_ZONES_ARTIFACT_ID` | não | `artifact_id` acompanhado em `ods.artifact.published` (padrão: a versão sem o `-N` final) |
| `S1_ZONES_SHA256` | não | confere o arquivo do ZONES na partida |
| `S1_REGISTRY_DIR` | não | pasta do registro de artefatos, para carregar versões novas anunciadas |
| `S1_CALIBRATION_FILE` | não | mensagens `ods.i3.calibration` aplicadas na partida |
| `S1_KAFKA_BOOTSTRAP_SERVERS` | sim | endereço do broker (`host:porta`) |
| `S1_KAFKA_GROUP_ID` | não | grupo de consumo (padrão: `S1-zonas-<artifact_id>`) |
| `S1_KAFKA_<CHAVE>` | não | qualquer outra chave do librdkafka: `_` vira `.` e `__` vira `_` (`S1_KAFKA_SECURITY_PROTOCOL` → `security.protocol`, `S1_KAFKA_LOG__LEVEL` → `log_level`) |
| `S1_TOPIC_<NOME>` | não | nome de tópico, se o TRA-1 não usar o nome do schema (ver `Topics`) |
| `S1_TIME_FIELD` | não | `captured_at` (especificação, padrão) ou `occurred_at` (guia do Kafka) |
| `S1_LOG_LEVEL` | não | `INFO` (padrão), `DEBUG`, `WARNING`... |
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Mapping, Optional, Tuple

from .domain import S1Service, ZonesArtifact
from .ingestion import (
    TIME_FIELDS,
    ArtifactRegistry,
    ContractError,
    default_artifact_id,
    load_messages,
    parse_calibration,
    parse_zones,
)
from .output import S1Publisher
from .runtime import KafkaBus, S1Runtime, Topics

logger = logging.getLogger("s1")

# --------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------

KAFKA_PREFIX = "S1_KAFKA_"

# Chaves do librdkafka que só valem para o consumidor; o produtor recebe as
# demais (endereço, segurança, credenciais).
CONSUMER_ONLY_KEYS = {
    "group.id",
    "group.instance.id",
    "auto.offset.reset",
    "enable.auto.commit",
    "auto.commit.interval.ms",
    "enable.auto.offset.store",
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "max.poll.interval.ms",
    "enable.partition.eof",
    "isolation.level",
}


class ConfigError(ValueError):
    """Configuração ausente ou inválida."""


@dataclass(frozen=True)
class S1Config:
    zones_file: Path
    zones_version: str
    zones_artifact_id: str
    kafka: Dict[str, str]
    zones_sha256: Optional[str] = None
    registry_dir: Optional[Path] = None
    calibration_file: Optional[Path] = None
    topics: Topics = field(default_factory=Topics)
    time_field: str = "captured_at"
    log_level: str = "INFO"

    @property
    def consumer_config(self) -> Dict[str, str]:
        return dict(self.kafka)

    @property
    def producer_config(self) -> Dict[str, str]:
        return {key: value for key, value in self.kafka.items() if key not in CONSUMER_ONLY_KEYS}

    @classmethod
    def from_env(cls, environ: Optional[Mapping[str, str]] = None) -> "S1Config":
        env = os.environ if environ is None else environ

        def get(name: str) -> Optional[str]:
            value = env.get(name, "").strip()
            return value or None

        def require(name: str) -> str:
            value = get(name)
            if value is None:
                raise ConfigError(f"variável de ambiente {name} é obrigatória")
            return value

        zones_version = require("S1_ZONES_VERSION")
        zones_artifact_id = get("S1_ZONES_ARTIFACT_ID") or default_artifact_id(zones_version)

        kafka = {
            kafka_key(key[len(KAFKA_PREFIX):]): value.strip()
            for key, value in env.items()
            if key.startswith(KAFKA_PREFIX) and value.strip()
        }
        if "bootstrap.servers" not in kafka:
            raise ConfigError(f"variável de ambiente {KAFKA_PREFIX}BOOTSTRAP_SERVERS é obrigatória (host:porta do broker)")
        # Cada instância do S1 precisa do seu grupo: duas instâncias no mesmo
        # grupo dividiriam as mensagens entre si (guia do Kafka).
        kafka.setdefault("group.id", f"S1-zonas-{zones_artifact_id}")
        # Ao vivo, um grupo novo começa do agora — não reprocessa o histórico.
        kafka.setdefault("auto.offset.reset", "latest")
        kafka.setdefault("client.id", kafka["group.id"])

        time_field = get("S1_TIME_FIELD") or "captured_at"
        if time_field not in TIME_FIELDS:
            raise ConfigError(f"S1_TIME_FIELD deve ser um de {TIME_FIELDS}, recebido {time_field!r}")

        return cls(
            zones_file=Path(require("S1_ZONES_FILE")),
            zones_version=zones_version,
            zones_artifact_id=zones_artifact_id,
            kafka=kafka,
            zones_sha256=get("S1_ZONES_SHA256"),
            registry_dir=Path(get("S1_REGISTRY_DIR")) if get("S1_REGISTRY_DIR") else None,
            calibration_file=Path(get("S1_CALIBRATION_FILE")) if get("S1_CALIBRATION_FILE") else None,
            topics=Topics.from_env(env),
            time_field=time_field,
            log_level=(get("S1_LOG_LEVEL") or "INFO").upper(),
        )


def kafka_key(env_suffix: str) -> str:
    """`SECURITY_PROTOCOL` → `security.protocol`. As poucas chaves do
    librdkafka com sublinhado usam dois: `LOG__LEVEL` → `log_level`."""

    return ".".join(part.replace("\0", "_") for part in env_suffix.lower().replace("__", "\0").split("_"))


# --------------------------------------------------------------------------
# Montagem na partida
# --------------------------------------------------------------------------

def load_pinned_zones(config: S1Config) -> ZonesArtifact:
    data = config.zones_file.read_bytes()
    if config.zones_sha256 and hashlib.sha256(data).hexdigest() != config.zones_sha256.lower():
        raise ContractError(f"hash_sha256 de {config.zones_file} não confere com S1_ZONES_SHA256")
    return parse_zones(json.loads(data), config.zones_version)


def build_runtime(config: S1Config) -> S1Runtime:
    zones = load_pinned_zones(config)
    service = S1Service(zones)
    if config.calibration_file is not None:
        for message in load_messages(config.calibration_file):
            service.add_calibration(parse_calibration(message))
    registry = ArtifactRegistry(config.registry_dir) if config.registry_dir else None
    logger.info(
        "ZONES %s (%s, %d zona(s)) para as câmeras %s",
        zones.artifact_version,
        zones.coordinates,
        len(zones.zones),
        ", ".join(zones.source_ids),
    )
    return S1Runtime(
        service,
        S1Publisher(time_field=config.time_field),
        zones_artifact_id=config.zones_artifact_id,
        registry=registry,
    )


def create_clients(config: S1Config) -> Tuple[object, object]:
    """Consumidor e produtor reais. A importação fica aqui dentro para o
    resto do S1 (simulador, testes) funcionar sem a biblioteca."""

    from confluent_kafka import Consumer, Producer

    return Consumer(config.consumer_config), Producer(config.producer_config)


def build_bus(config: S1Config, runtime: S1Runtime, clients: Optional[Tuple[object, object]] = None) -> KafkaBus:
    consumer, producer = clients or create_clients(config)
    return KafkaBus(runtime, consumer, producer, config.topics)


# --------------------------------------------------------------------------
# Serviço
# --------------------------------------------------------------------------

def main() -> int:
    try:
        config = S1Config.from_env()
    except ConfigError as error:
        logging.basicConfig(level=logging.INFO)
        logger.error("configuração inválida: %s", error)
        return 2

    logging.basicConfig(level=config.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        runtime = build_runtime(config)
    except (OSError, ValueError) as error:
        logger.error("não foi possível carregar o ZONES/calibração: %s", error)
        return 2

    stop = threading.Event()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_number, lambda *_: stop.set())

    logger.info("S1 conectando a %s como %s", config.kafka["bootstrap.servers"], config.kafka["group.id"])
    try:
        bus = build_bus(config, runtime)
    except Exception as error:  # confluent_kafka.KafkaException: chave S1_KAFKA_* inválida
        logger.error("configuração do Kafka rejeitada: %s", error)
        return 2
    bus.run(stop.is_set)
    return 0


if __name__ == "__main__":
    sys.exit(main())
