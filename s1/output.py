"""
Saída do S1: mensagem `ods.visao.evento_espacial`.

Mesmo envelope usado por I2/I3 (`message_type`, `schema`,
`schema_version`, `producer`, `published_at`, `payload`). É publicada
uma mensagem para cada leitura processada pelo `S1Service`
(entity_id/source_id/timestamp/world_coordinates são sempre
obrigatórios), enriquecida com:

- `zone_id`: presente apenas quando a entidade está, neste instante,
  confirmada dentro de alguma zona.
- `transition`: presente apenas quando esta leitura corresponde a uma
  transição de zona confirmada pela histerese (entrada, saída ou troca
  direta entre zonas).

`world_coordinates` é obrigatório no contrato, mas nem toda leitura tem
uma posição de mundo (instalação simples, zonas em pixel, sem
calibração de I3 — ver `s1/ingestion.py`). Quando não há
`world_position`, publicamos a posição em pixel nesse campo: é a
melhor informação de localização disponível, e mantém o payload
sempre válido em relação ao contrato.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from .domain import TrajectoryPoint, ZoneEvent

SPATIAL_EVENT_SCHEMA = "ods.visao.evento_espacial"
SPATIAL_EVENT_SCHEMA_VERSION = "1.0"
SPATIAL_EVENT_PRODUCER = "S1"

_TRANSITION_TYPE_BY_EVENT_TYPE = {
    "enter": "entrada",
    "exit": "saida",
    "transfer": "transferencia",
}

def _format_timestamp(timestamp: datetime) -> str:
    """ISO-8601 com milissegundos e sufixo 'Z' (ex: 2026-09-16T10:00:00.000Z)."""

    millis = timestamp.microsecond // 1000
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.") + f"{millis:03d}Z"


def _build_transition(event: ZoneEvent) -> Optional[Dict]:
    transition_type = _TRANSITION_TYPE_BY_EVENT_TYPE.get(event.event_type)
    if transition_type is None:
        return None
    transition: Dict = {"type": transition_type}
    if event.previous_zone_id is not None:
        transition["previous_zone_id"] = event.previous_zone_id
    return transition


def to_spatial_event(
    point: TrajectoryPoint, event: ZoneEvent, published_at: Optional[datetime] = None
) -> Dict:
    """Monta a mensagem `ods.visao.evento_espacial` para uma leitura
    processada pelo S1Service.

    `published_at` é o instante de publicação da mensagem; por padrão
    usa o timestamp da própria leitura.
    """
    location = point.world_position or point.pixel_position
    payload: Dict = {
        "entity_id": point.entity_id,
        "source_id": point.source_id,
        "timestamp": _format_timestamp(point.timestamp),
        "world_coordinates": {"x": location.x, "y": location.y},
    }

    if event.zone_id is not None:
        payload["zone_id"] = event.zone_id

    transition = _build_transition(event)
    if transition is not None:
        payload["transition"] = transition

    return {
        "message_type": "event",
        "schema": SPATIAL_EVENT_SCHEMA,
        "schema_version": SPATIAL_EVENT_SCHEMA_VERSION,
        "producer": SPATIAL_EVENT_PRODUCER,
        "published_at": _format_timestamp(published_at or point.timestamp),
        "payload": payload,
    }
