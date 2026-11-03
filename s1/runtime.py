from __future__ import annotations

import json
import logging
from collections import OrderedDict
from dataclasses import dataclass, fields
from typing import Callable, Dict, List, Mapping, Optional

from .domain import S1Service
from .ingestion import (
    ARTIFACT_PUBLISHED,
    I2_TRACKS,
    I3_CALIBRATION,
    SESSION_STARTED,
    ArtifactRegistry,
    ContractError,
    decode_message,
    parse_artifact_published,
    parse_calibration,
    parse_session_started,
    parse_tracks,
    parse_zones,
)
from .output import POSITION, ZONE_OCCUPANCY, ZONE_TRANSITION, S1Publisher

logger = logging.getLogger(__name__)

class S1Runtime:
    def __init__(
        self,
        service: S1Service,
        publisher: S1Publisher,
        zones_artifact_id: str,
        registry: Optional[ArtifactRegistry] = None,
        dedup_capacity: int = 10_000,
    ):
        self._service = service
        self._publisher = publisher
        self._zones_artifact_id = zones_artifact_id
        self._registry = registry
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self._dedup_capacity = dedup_capacity

    def handle(self, message: Dict) -> List[Dict]:
        message_id = message.get("message_id")
        if message_id is not None:
            if message_id in self._seen:
                return []
            self._seen[message_id] = None
            if len(self._seen) > self._dedup_capacity:
                self._seen.popitem(last=False)

        try:
            return self._dispatch(message)
        except (ValueError, OSError) as error:
            logger.warning("mensagem %s (%s) descartada: %s", message_id, message.get("schema"), error)
            return []

    def _dispatch(self, message: Dict) -> List[Dict]:
        schema = message.get("schema")

        if schema == I2_TRACKS:
            result = self._service.process_frame(parse_tracks(message))
            return self._publisher.frame_messages(result) if result else []

        if schema == I3_CALIBRATION:
            self._service.add_calibration(parse_calibration(message))
            return []

        if schema == SESSION_STARTED:
            session = parse_session_started(message)
            reset = self._service.start_session(session.session_id, list(session.sources), session.started_at)
            return self._publisher.occupancy_messages(reset, message.get("message_id"))

        if schema == ARTIFACT_PUBLISHED:
            descriptor = parse_artifact_published(message)
            if descriptor.kind == "ZONES" and descriptor.artifact_id == self._zones_artifact_id:
                if self._registry is None:
                    raise ContractError("ZONES anunciado, mas o S1 está sem registro de artefatos")
                content = self._registry.load(descriptor)
                self._service.schedule_zones(parse_zones(content, descriptor.artifact_version))
            return []

        return []

# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------

TOPIC_PREFIX = "S1_TOPIC_"

@dataclass(frozen=True)
class Topics:
    i2_tracks: str = I2_TRACKS
    i3_calibration: str = I3_CALIBRATION
    session_started: str = SESSION_STARTED
    artifact_published: str = ARTIFACT_PUBLISHED
    position: str = POSITION
    zone_transition: str = ZONE_TRANSITION
    zone_occupancy: str = ZONE_OCCUPANCY

    def inputs(self) -> List[str]:
        return list(dict.fromkeys([self.i2_tracks, self.i3_calibration, self.session_started, self.artifact_published]))

    def output_for(self, schema: str) -> str:
        return {POSITION: self.position, ZONE_TRANSITION: self.zone_transition, ZONE_OCCUPANCY: self.zone_occupancy}[
            schema
        ]

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> "Topics":
        overrides = {}
        for topic_field in fields(cls):
            value = environ.get(TOPIC_PREFIX + topic_field.name.upper(), "").strip()
            if value:
                overrides[topic_field.name] = value
        return cls(**overrides)


def encode_message(message: Dict) -> bytes:
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

class KafkaBus:
    def __init__(self, runtime: S1Runtime, consumer, producer, topics: Topics, poll_timeout_s: float = 1.0):
        self._runtime = runtime
        self._consumer = consumer
        self._producer = producer
        self._topics = topics
        self._poll_timeout_s = poll_timeout_s
        self.delivery_failures = 0

    def start(self) -> None:
        topics = self._topics.inputs()
        self._consumer.subscribe(topics)
        logger.info("S1 assinando %s", ", ".join(topics))

    def poll_once(self) -> int:
        record = self._consumer.poll(self._poll_timeout_s)
        self._producer.poll(0)  # atende os avisos de entrega pendentes
        if record is None:
            return 0
        error = record.error()
        if error is not None:
            if getattr(error, "fatal", lambda: False)():
                raise RuntimeError(f"erro fatal do Kafka: {error}")
            logger.warning("erro do Kafka em %s: %s", record.topic(), error)
            return 0

        try:
            message = decode_message(record.value())
        except ContractError as problem:
            logger.warning("mensagem descartada em %s (offset %s): %s", record.topic(), record.offset(), problem)
            return 0
        try:
            outputs = self._runtime.handle(message)
        except Exception:  # um defeito numa mensagem não derruba o serviço
            logger.exception("falha inesperada ao processar %s (%s)", message.get("message_id"), message.get("schema"))
            return 0

        for output in outputs:
            self.publish(output)
        return len(outputs)

    def publish(self, message: Dict) -> None:
        topic = self._topics.output_for(message["schema"])
        key = message["source_id"].encode("utf-8")
        value = encode_message(message)
        try:
            self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
        except BufferError:
            self._producer.poll(1.0)
            self._producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)

    def run(self, should_stop: Callable[[], bool]) -> None:
        self.start()
        try:
            while not should_stop():
                self.poll_once()
        finally:
            self.close()

    def close(self, flush_timeout_s: float = 10.0) -> None:
        pending = self._producer.flush(flush_timeout_s)
        if pending:
            logger.warning("%d mensagem(ns) do S1 não foram entregues antes de fechar", pending)
        self._consumer.close()
        logger.info("S1 desconectado do barramento")

    def _on_delivery(self, error, record) -> None:
        if error is not None:
            self.delivery_failures += 1
            logger.error("falha ao publicar em %s: %s", record.topic() if record else "?", error)