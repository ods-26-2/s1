"""Construtores de zonas, quadros e mensagens usados pelos testes."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from s1.domain import PIXELS, Frame, Point, TrackObservation, Zone, ZonesArtifact
from s1.output import format_timestamp

T0 = datetime(2026, 9, 23, 14, 0, 0, tzinfo=timezone.utc)
SESSION = "11111111-1111-4111-8111-111111111111"


def at(ms: float) -> datetime:
    return T0 + timedelta(milliseconds=ms)


def square(zone_id, x0, y0, x1, y1, enter_ms=100, exit_ms=100, **kwargs) -> Zone:
    return Zone(
        zone_id=zone_id,
        name=zone_id,
        polygon=(Point(x0, y0), Point(x1, y0), Point(x1, y1), Point(x0, y1)),
        enter_hysteresis_ms=enter_ms,
        exit_hysteresis_ms=exit_ms,
        **kwargs,
    )


def artifact(*zones, coordinates=PIXELS, source_ids=("cam0",), space_id=None, version="zones-t-1") -> ZonesArtifact:
    return ZonesArtifact(
        artifact_version=version,
        coordinates=coordinates,
        source_ids=tuple(source_ids),
        zones=tuple(zones),
        space_id=space_id,
    )


def track(track_id, x, y, object_class="person", lost=False) -> TrackObservation:
    return TrackObservation(track_id, object_class, Point(x, y), lost)


def frame(index, *tracks, ms=None, source="cam0", session=SESSION, message_id=None) -> Frame:
    return Frame(
        source_id=source,
        session_id=session,
        frame_index=index,
        captured_at=at(index * 100 if ms is None else ms),
        tracks=tuple(tracks),
        message_id=message_id,
    )


def tracks_message(
    index: int, tracks: List[Dict], ms: Optional[float] = None, source="cam0", session=SESSION, message_id=None
) -> Dict:
    captured = at(index * 100 if ms is None else ms)
    return {
        "message_type": "event",
        "message_id": message_id or str(uuid.uuid4()),
        "schema": "ods.i2.tracks",
        "schema_version": "1.0",
        "producer": "I2",
        "published_at": format_timestamp(captured),
        "captured_at": format_timestamp(captured),
        "source_id": source,
        "session_id": session,
        "sequence": index,
        "artifact_version": "bytetrack-1.2.0",
        "payload": {"frame_index": index, "tracks": tracks},
    }


def raw_track(track_id, x, y, object_class="person", state="CONFIRMED") -> Dict:
    return {
        "track_id": track_id,
        "class": object_class,
        "state": state,
        "predicted": False,
        "confidence": 0.9,
        "point_px": [x, y],
        "bbox": [x - 40, y - 180, x + 40, y],
    }


def session_message(session_id: str, sources=("cam0",), ms: float = 0, message_id=None) -> Dict:
    started = format_timestamp(at(ms))
    return {
        "message_type": "event",
        "message_id": message_id or str(uuid.uuid4()),
        "schema": "ods.session.started",
        "schema_version": "1.0",
        "producer": "P6",
        "published_at": started,
        "captured_at": started,
        "source_id": sources[0],
        "session_id": session_id,
        "sequence": 0,
        "payload": {"mode": "REPLAY", "started_at": started, "sources": list(sources), "reason": "SEEK"},
    }


def calibration_message(source="cam0", version="cal-cam0-1", space_id="patio-1", scale=0.01) -> Dict:
    return {
        "message_type": "event",
        "message_id": str(uuid.uuid4()),
        "schema": "ods.i3.calibration",
        "schema_version": "1.0",
        "producer": "I3",
        "published_at": "2026-09-01T00:00:00.000Z",
        "source_id": source,
        "sequence": 0,
        "artifact_version": version,
        "payload": {
            "valid_from": "2026-09-01T00:00:00.000Z",
            "image_width": 1920,
            "image_height": 1080,
            "intrinsics": {"fx": 1456.2, "fy": 1458.9, "cx": 962.4, "cy": 541.7},
            "distortion": [-0.312, 0.108, 0.0004, -0.0002, -0.017],
            "homography_px_to_m": [[scale, 0.0, 0.0], [0.0, scale, 0.0], [0.0, 0.0, 1.0]],
            "reference_frame": {"space_id": space_id, "origin": "canto", "axes": "x, y"},
            "reprojection_rms_cm": 4.2,
            "holdout_points": 12,
        },
    }
