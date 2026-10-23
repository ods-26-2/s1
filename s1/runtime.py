"""Ponte entre o barramento e o S1: recebe cada mensagem assinada, descarta
repetidas pelo message_id, encaminha pelo schema e devolve as mensagens a
publicar.

Assinaturas do S1 (arquitetura 6c): ods.i2.tracks, ods.i3.calibration,
ods.session.started e ods.artifact.published. A arquitetura também permite
ods.i1.detections e ods.p6.replay.state, mas nenhuma saída do S1 depende
deles; mensagens de outros schemas são ignoradas.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from typing import Dict, List, Optional

from .domain import S1Service
from .ingestion import (
    ARTIFACT_PUBLISHED,
    I2_TRACKS,
    I3_CALIBRATION,
    SESSION_STARTED,
    ArtifactRegistry,
    ContractError,
    parse_artifact_published,
    parse_calibration,
    parse_session_started,
    parse_tracks,
    parse_zones,
)
from .output import S1Publisher

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
            # CALIBRATION também chega com o conteúdo em ods.i3.calibration.
            if descriptor.kind == "ZONES" and descriptor.artifact_id == self._zones_artifact_id:
                if self._registry is None:
                    raise ContractError("ZONES anunciado, mas o S1 está sem registro de artefatos")
                content = self._registry.load(descriptor)
                self._service.schedule_zones(parse_zones(content, descriptor.artifact_version))
            return []

        return []
