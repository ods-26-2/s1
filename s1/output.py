"""
Saída do S1: as três mensagens da Especificação de interfaces 1.0, no
envelope comum (perfil EV-SINAL).

- `ods.s1.position`: uma por quadro e câmera, com a lista completa dos
  rastros CONFIRMED (inclusive vazia).
- `ods.s1.zone_transition`: uma por entrada ou saída confirmada;
  `captured_at` é o instante do cruzamento.
- `ods.s1.zone_occupancy`: uma por zona e câmera a cada mudança de contagem.

`artifact_version` é a versão do ZONES que produziu cada mensagem. `sequence` conta as
mensagens do S1 por sessão, a partir de 0. `correlation_id` aponta para a
mensagem que causou a publicação (o quadro de I2 ou o início de sessão).
O payload nunca repete um campo do envelope.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional

from .domain import FrameResult, Occupancy, Point, PositionItem, Transition

POSITION = "ods.s1.position"
ZONE_TRANSITION = "ods.s1.zone_transition"
ZONE_OCCUPANCY = "ods.s1.zone_occupancy"
SCHEMA_VERSION = "1.0"
PRODUCER = "S1"


def format_timestamp(timestamp: datetime) -> str:
    """ISO 8601 em UTC com milissegundos (2026-09-23T14:29:59.870Z)."""

    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.") + f"{timestamp.microsecond // 1000:03d}Z"


def _xy(point: Point) -> List[float]:
    return [point.x, point.y]


class S1Publisher:
    def __init__(
        self,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        new_message_id: Callable[[], str] = lambda: str(uuid.uuid4()),
    ):
        self._clock = clock
        self._new_message_id = new_message_id
        self._sequence: Dict[str, int] = {}

    def frame_messages(self, result: FrameResult) -> List[Dict]:
        """Posição do quadro, depois as transições, depois a ocupação."""

        frame, artifact_version = result.frame, result.artifact_version
        messages = [
            self._envelope(
                POSITION,
                captured_at=frame.captured_at,
                source_id=frame.source_id,
                session_id=frame.session_id,
                artifact_version=artifact_version,
                correlation_id=frame.message_id,
                payload={
                    "frame_index": frame.frame_index,
                    "positions": [self._position(item) for item in result.positions],
                },
            )
        ]
        messages += [
            self._transition(transition, frame.session_id, artifact_version, frame.message_id)
            for transition in result.transitions
        ]
        messages += self.occupancy_messages(result.occupancy_changes, frame.message_id)
        return messages

    def occupancy_messages(self, changes: Iterable[Occupancy], correlation_id: Optional[str] = None) -> List[Dict]:
        return [
            self._envelope(
                ZONE_OCCUPANCY,
                captured_at=change.at,
                source_id=change.source_id,
                session_id=change.session_id,
                artifact_version=change.artifact_version,
                correlation_id=correlation_id,
                payload=self._occupancy(change),
            )
            for change in changes
        ]

    # ----------------------------------------------------------------------

    @staticmethod
    def _position(item: PositionItem) -> Dict:
        payload = {"track_id": item.track_id, "class": item.object_class, "point_px": _xy(item.point_px)}
        if item.position_m is not None:
            payload["position_m"] = _xy(item.position_m)
            payload["space_id"] = item.space_id
        payload["zone_ids"] = list(item.zone_ids)
        return payload

    def _transition(
        self, transition: Transition, session_id: str, artifact_version: str, correlation_id: Optional[str]
    ) -> Dict:
        payload = {
            "track_id": transition.track_id,
            "class": transition.object_class,
            "zone_id": transition.zone_id,
            "transition": transition.kind,
            "entered_at": format_timestamp(transition.entered_at),
        }
        if transition.dwell_s is not None:
            payload["dwell_s"] = transition.dwell_s
        return self._envelope(
            ZONE_TRANSITION,
            captured_at=transition.at,
            source_id=transition.source_id,
            session_id=session_id,
            artifact_version=artifact_version,
            correlation_id=correlation_id,
            payload=payload,
        )

    @staticmethod
    def _occupancy(change: Occupancy) -> Dict:
        payload = {"zone_id": change.zone_id, "count": change.count, "count_by_class": dict(change.count_by_class)}
        if change.max_occupancy is not None:
            payload["max_occupancy"] = change.max_occupancy
        payload["track_ids"] = list(change.track_ids)
        return payload

    def _envelope(
        self,
        schema: str,
        *,
        captured_at: datetime,
        source_id: str,
        session_id: str,
        artifact_version: str,
        payload: Dict,
        correlation_id: Optional[str] = None,
    ) -> Dict:
        sequence = self._sequence.get(session_id, 0)
        self._sequence[session_id] = sequence + 1
        message = {
            "message_type": "event",
            "message_id": self._new_message_id(),
            "schema": schema,
            "schema_version": SCHEMA_VERSION,
            "producer": PRODUCER,
        }
        if correlation_id is not None:
            message["correlation_id"] = correlation_id
        message.update(
            {
                "published_at": format_timestamp(self._clock()),
                "captured_at": format_timestamp(captured_at),
                "source_id": source_id,
                "session_id": session_id,
                "sequence": sequence,
                "artifact_version": artifact_version,
                "payload": payload,
            }
        )
        return message
