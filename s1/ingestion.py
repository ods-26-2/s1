"""Entrada do S1: contratos consumidos e artefatos carregados.

Mensagens (Especificação de interfaces 1.0, arquitetura 6c), todas no
envelope comum — câmera, sessão e tempo de captura vêm no envelope:

- `ods.i2.tracks`: rastros de um quadro de uma câmera.
- `ods.i3.calibration`: conteúdo de um artefato CALIBRATION publicado.
- `ods.session.started`: início de sessão (ao vivo, ou replay após
  START/SEEK/LOOP/RESTART).
- `ods.artifact.published`: anúncio de nova versão de artefato.

Artefatos (registro de B5, imutáveis e versionados): ZONES e CALIBRATION.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Union

from .domain import Calibration, Frame, Homography, Point, TrackObservation, Zone, ZonesArtifact

PathLike = Union[str, Path]

I2_TRACKS = "ods.i2.tracks"
I3_CALIBRATION = "ods.i3.calibration"
SESSION_STARTED = "ods.session.started"
ARTIFACT_PUBLISHED = "ods.artifact.published"

_TRACK_STATES = ("CONFIRMED", "LOST")


class ContractError(ValueError):
    """Mensagem ou artefato fora do contrato."""


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _require(obj: Dict, key: str, where: str):
    if obj.get(key) is None:
        raise ContractError(f"{where}: campo obrigatório {key!r} ausente")
    return obj[key]


def _require_int(obj: Dict, key: str, where: str) -> int:
    value = _require(obj, key, where)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ContractError(f"{where}: {key!r} deve ser inteiro, recebido {value!r}")
    return value


def _point(value, where: str) -> Point:
    try:
        x, y = value
        return Point(float(x), float(y))
    except (TypeError, ValueError) as error:
        raise ContractError(f"{where}: esperado [x, y], recebido {value!r}") from error


def _expect_schema(message: Dict, schema: str) -> Dict:
    if message.get("schema") != schema:
        raise ContractError(f"esperado schema {schema!r}, recebido {message.get('schema')!r}")
    return _require(message, "payload", schema)


def parse_tracks(message: Dict) -> Frame:
    payload = _expect_schema(message, I2_TRACKS)
    tracks = []
    for index, raw in enumerate(_require(payload, "tracks", I2_TRACKS)):
        where = f"{I2_TRACKS} tracks[{index}]"
        state = _require(raw, "state", where)
        if state not in _TRACK_STATES:
            raise ContractError(f"{where}: state deve ser CONFIRMED ou LOST, recebido {state!r}")
        tracks.append(
            TrackObservation(
                track_id=_require(raw, "track_id", where),
                object_class=_require(raw, "class", where),
                point_px=_point(_require(raw, "point_px", where), where),
                lost=state == "LOST",
            )
        )
    return Frame(
        source_id=_require(message, "source_id", I2_TRACKS),
        session_id=_require(message, "session_id", I2_TRACKS),
        frame_index=_require_int(payload, "frame_index", I2_TRACKS),
        captured_at=parse_timestamp(_require(message, "captured_at", I2_TRACKS)),
        tracks=tuple(tracks),
        message_id=message.get("message_id"),
    )


def calibration_from_content(content: Dict, source_id: str, artifact_version: str) -> Calibration:
    reference_frame = _require(content, "reference_frame", "CALIBRATION")
    try:
        homography = Homography(_require(content, "homography_px_to_m", "CALIBRATION"))
    except ValueError as error:
        raise ContractError(f"CALIBRATION: {error}") from error
    return Calibration(
        source_id=source_id,
        artifact_version=artifact_version,
        valid_from=parse_timestamp(_require(content, "valid_from", "CALIBRATION")),
        homography=homography,
        space_id=_require(reference_frame, "space_id", "CALIBRATION reference_frame"),
    )


def parse_calibration(message: Dict) -> Calibration:
    payload = _expect_schema(message, I3_CALIBRATION)
    return calibration_from_content(
        payload,
        source_id=_require(message, "source_id", I3_CALIBRATION),
        artifact_version=_require(message, "artifact_version", I3_CALIBRATION),
    )


@dataclass(frozen=True)
class SessionStart:
    session_id: str
    started_at: datetime
    sources: Tuple[str, ...]


def parse_session_started(message: Dict) -> SessionStart:
    payload = _expect_schema(message, SESSION_STARTED)
    return SessionStart(
        session_id=_require(message, "session_id", SESSION_STARTED),
        started_at=parse_timestamp(_require(payload, "started_at", SESSION_STARTED)),
        sources=tuple(_require(payload, "sources", SESSION_STARTED)),
    )


@dataclass(frozen=True)
class ArtifactDescriptor:
    artifact_id: str
    artifact_version: str
    kind: str
    owner: str
    uri: str
    hash_sha256: str
    valid_from: datetime
    schema_version: str


def parse_artifact_descriptor(data: Dict) -> ArtifactDescriptor:
    where = "descritor de artefato"
    return ArtifactDescriptor(
        artifact_id=_require(data, "artifact_id", where),
        artifact_version=_require(data, "artifact_version", where),
        kind=_require(data, "kind", where),
        owner=_require(data, "owner", where),
        uri=_require(data, "uri", where),
        hash_sha256=_require(data, "hash_sha256", where),
        valid_from=parse_timestamp(_require(data, "valid_from", where)),
        schema_version=_require(data, "schema_version", where),
    )


def parse_artifact_published(message: Dict) -> ArtifactDescriptor:
    return parse_artifact_descriptor(_expect_schema(message, ARTIFACT_PUBLISHED))

def _parse_zone(raw: Dict, index: int) -> Zone:
    zone_id = _require(raw, "zone_id", f"ZONES zones[{index}]")
    where = f"ZONES zona {zone_id!r}"
    return Zone(
        zone_id=zone_id,
        name=_require(raw, "name", where),
        polygon=tuple(_point(vertex, where) for vertex in _require(raw, "polygon", where)),
        enter_hysteresis_ms=_require_int(raw, "enter_hysteresis_ms", where),
        exit_hysteresis_ms=_require_int(raw, "exit_hysteresis_ms", where),
        max_occupancy=raw.get("max_occupancy"),
        classes=tuple(raw.get("classes") or ()),
    )


def parse_zones(content: Dict, artifact_version: str) -> ZonesArtifact:
    try:
        source_ids = _require(content, "source_ids", "ZONES")
        return ZonesArtifact(
            artifact_version=artifact_version,
            coordinates=_require(content, "coordinates", "ZONES"),
            source_ids=tuple(source_ids),
            zones=tuple(_parse_zone(raw, index) for index, raw in enumerate(_require(content, "zones", "ZONES"))),
            space_id=content.get("space_id"),
        )
    except ContractError:
        raise
    except ValueError as error:
        raise ContractError(f"ZONES: {error}") from error

# --------------------------------------------------------------------------
# Registro de artefatos. O registro real é de B5; aqui, uma pasta local em
# que a URI registry://<caminho> aponta para <raiz>/<caminho>. O conteúdo
# só é aceito se o sha256 bater com o do descritor.
# --------------------------------------------------------------------------

class ArtifactRegistry:
    def __init__(self, root: PathLike):
        self._root = Path(root)

    def load(self, descriptor: ArtifactDescriptor) -> Dict:
        prefix = "registry://"
        if not descriptor.uri.startswith(prefix):
            raise ContractError(f"URI fora do registro: {descriptor.uri!r}")
        data = (self._root / descriptor.uri[len(prefix):]).read_bytes()
        if hashlib.sha256(data).hexdigest() != descriptor.hash_sha256:
            raise ContractError(f"hash_sha256 não confere para {descriptor.artifact_version}")
        return json.loads(data)


def load_json(path: PathLike):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_messages(path: PathLike) -> List[Dict]:
    """Um arquivo com uma lista de mensagens, na ordem de chegada."""

    return list(load_json(path))
