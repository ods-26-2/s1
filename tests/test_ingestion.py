"""Testes da ingestão do S1: parsing de I2/I3, homografia, registro de
calibração e a ponte até TrajectoryPoint (s1/ingestion.py)."""

from datetime import datetime, timezone

import pytest

from s1.ingestion import (
    CalibrationEvent,
    CalibrationNotFoundError,
    CalibrationRegistry,
    DegenerateHomographyError,
    Homography,
    Point,
    RawTrack,
    ReferenceFrame,
    SchemaMismatchError,
    TrackEvent,
    parse_calibration_event,
    parse_track_event,
    to_trajectory_points,
)

_REFERENCE_FRAME = ReferenceFrame(
    space_id="galpao_principal",
    origin="canto_inferior_esquerdo_planta",
    axes="x_direita_y_cima",
    unit="metros",
)

# --------------------------------------------------------------------------
# Parsing dos envelopes de I2/I3
# --------------------------------------------------------------------------


def _track_message(**overrides):
    payload = {
        "camera_id": "cam_frontal",
        "session_id": "sess_001",
        "captured_at": "2026-09-16T10:47:28.100Z",
        "frame": 100,
        "tracks": [
            {
                "track_id": 1,
                "class": "person",
                "state": "confirmed",
                "u_px": 509.0,
                "v_px": 250.0,
                "predicted": False,
            }
        ],
    }
    payload.update(overrides)
    return {
        "message_type": "event",
        "schema": "ods.inferencia.rastreio",
        "schema_version": "1.0",
        "producer": "I2",
        "published_at": "2026-09-16T10:47:28.150Z",
        "payload": payload,
    }


def _calibration_message(**overrides):
    payload = {
        "camera_id": "cam_frontal",
        "calibration_version": "calib-frontal-v1",
        "valid_from": "2026-09-01T00:00:00.000Z",
        "homography": [[0.1, 0.0, 0.0], [0.0, 0.1, 0.0], [0.0, 0.0, 1.0]],
        "reference_frame": {
            "space_id": "galpao_principal",
            "origin": "canto_inferior_esquerdo_planta",
            "axes": "x_direita_y_cima",
            "unit": "metros",
        },
        "reprojection_rms_cm": 1.8,
        "holdout_points": 12,
    }
    payload.update(overrides)
    return {
        "message_type": "event",
        "schema": "ods.inferencia.calibracao",
        "schema_version": "1.0",
        "producer": "I3",
        "published_at": "2026-09-01T00:00:00.500Z",
        "payload": payload,
    }


def test_parse_track_event_extracts_camera_and_tracks():
    event = parse_track_event(_track_message())
    assert event.camera_id == "cam_frontal"
    assert event.session_id == "sess_001"
    assert event.frame == 100
    assert len(event.tracks) == 1
    track = event.tracks[0]
    assert track.track_id == 1
    assert track.object_class == "person"
    assert track.state == "confirmed"
    assert track.u_px == 509.0
    assert track.v_px == 250.0
    assert track.predicted is False


def test_parse_track_event_rejects_wrong_schema():
    message = _track_message()
    message["schema"] = "ods.inferencia.calibracao"
    with pytest.raises(SchemaMismatchError):
        parse_track_event(message)


def test_parse_calibration_event_extracts_homography_and_reference_frame():
    event = parse_calibration_event(_calibration_message())
    assert event.camera_id == "cam_frontal"
    assert event.calibration_version == "calib-frontal-v1"
    assert event.homography == [[0.1, 0.0, 0.0], [0.0, 0.1, 0.0], [0.0, 0.0, 1.0]]
    assert event.reference_frame.unit == "metros"
    assert event.reprojection_rms_cm == 1.8
    assert event.holdout_points == 12


def test_parse_calibration_event_rejects_wrong_schema():
    message = _calibration_message()
    message["schema"] = "ods.inferencia.rastreio"
    with pytest.raises(SchemaMismatchError):
        parse_calibration_event(message)


# --------------------------------------------------------------------------
# Homography (projeção pixel -> mundo)
# --------------------------------------------------------------------------


def test_project_applies_scale():
    homography = Homography([[0.1, 0.0, 0.0], [0.0, 0.1, 0.0], [0.0, 0.0, 1.0]])
    point = homography.project(509.0, 250.0)
    assert point.x == pytest.approx(50.9)
    assert point.y == pytest.approx(25.0)


def test_project_applies_offset_and_scale():
    homography = Homography([[0.2, 0.0, 10.0], [0.0, 0.2, 5.0], [0.0, 0.0, 1.0]])
    point = homography.project(250.0, 100.0)
    assert point.x == pytest.approx(60.0)
    assert point.y == pytest.approx(25.0)


def test_project_applies_perspective_divide():
    # w varia com o pixel: simula uma homografia com componente de perspectiva.
    homography = Homography([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.001, 1.0]])
    point = homography.project(100.0, 100.0)
    expected_w = 0.001 * 100.0 + 1.0
    assert point.x == pytest.approx(100.0 / expected_w)
    assert point.y == pytest.approx(100.0 / expected_w)


def test_degenerate_homography_raises():
    homography = Homography([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 0.0]])
    with pytest.raises(DegenerateHomographyError):
        homography.project(10.0, 10.0)


def test_rejects_non_3x3_matrix():
    with pytest.raises(ValueError):
        Homography([[1.0, 0.0], [0.0, 1.0]])


# --------------------------------------------------------------------------
# CalibrationRegistry (resolução da calibração vigente por câmera/tempo)
# --------------------------------------------------------------------------


def _calibration_event(camera_id, version, valid_from_iso, scale=0.1):
    return CalibrationEvent(
        camera_id=camera_id,
        calibration_version=version,
        valid_from=datetime.fromisoformat(valid_from_iso).replace(tzinfo=timezone.utc),
        homography=[[scale, 0.0, 0.0], [0.0, scale, 0.0], [0.0, 0.0, 1.0]],
        reference_frame=_REFERENCE_FRAME,
        reprojection_rms_cm=1.0,
        holdout_points=10,
        published_at=datetime.fromisoformat(valid_from_iso).replace(tzinfo=timezone.utc),
    )


def test_resolve_returns_calibration_valid_at_timestamp():
    registry = CalibrationRegistry()
    registry.register(_calibration_event("cam_frontal", "v1", "2026-09-01T00:00:00"))

    calibration = registry.resolve(
        "cam_frontal", datetime(2026, 9, 16, 10, 0, 0, tzinfo=timezone.utc)
    )
    assert calibration.calibration_version == "v1"


def test_resolve_picks_latest_version_valid_before_timestamp():
    registry = CalibrationRegistry()
    registry.register(_calibration_event("cam_frontal", "v1", "2026-09-01T00:00:00", scale=0.1))
    registry.register(_calibration_event("cam_frontal", "v2", "2026-09-20T00:00:00", scale=0.2))

    before_recalibration = registry.resolve(
        "cam_frontal", datetime(2026, 9, 10, tzinfo=timezone.utc)
    )
    after_recalibration = registry.resolve(
        "cam_frontal", datetime(2026, 9, 25, tzinfo=timezone.utc)
    )

    assert before_recalibration.calibration_version == "v1"
    assert after_recalibration.calibration_version == "v2"


def test_resolve_raises_when_no_calibration_registered_for_camera():
    registry = CalibrationRegistry()
    registry.register(_calibration_event("cam_frontal", "v1", "2026-09-01T00:00:00"))

    with pytest.raises(CalibrationNotFoundError):
        registry.resolve("cam_lateral", datetime(2026, 9, 16, tzinfo=timezone.utc))


def test_resolve_raises_when_timestamp_precedes_first_calibration():
    registry = CalibrationRegistry()
    registry.register(_calibration_event("cam_frontal", "v1", "2026-09-20T00:00:00"))

    with pytest.raises(CalibrationNotFoundError):
        registry.resolve("cam_frontal", datetime(2026, 9, 1, tzinfo=timezone.utc))


# --------------------------------------------------------------------------
# to_trajectory_points (I2 + calibração -> TrajectoryPoint)
# --------------------------------------------------------------------------


def _registry_with_cam_frontal():
    registry = CalibrationRegistry()
    registry.register(
        CalibrationEvent(
            camera_id="cam_frontal",
            calibration_version="calib-frontal-v1",
            valid_from=datetime(2026, 9, 1, tzinfo=timezone.utc),
            homography=[[0.1, 0.0, 0.0], [0.0, 0.1, 0.0], [0.0, 0.0, 1.0]],
            reference_frame=_REFERENCE_FRAME,
            reprojection_rms_cm=1.8,
            holdout_points=12,
            published_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        )
    )
    return registry


def test_to_trajectory_points_projects_each_track_and_derives_entity_id():
    event = TrackEvent(
        camera_id="cam_frontal",
        session_id="sess_001",
        captured_at=datetime(2026, 9, 16, 10, 47, 28, 100_000, tzinfo=timezone.utc),
        frame=100,
        tracks=[
            RawTrack(track_id=1, object_class="person", state="confirmed", u_px=509.0, v_px=250.0, predicted=False),
            RawTrack(track_id=2, object_class="car", state="confirmed", u_px=600.0, v_px=100.0, predicted=False),
        ],
        published_at=datetime(2026, 9, 16, 10, 47, 28, 150_000, tzinfo=timezone.utc),
    )

    points = to_trajectory_points(event, _registry_with_cam_frontal())

    assert len(points) == 2
    assert points[0].entity_id == "cam_frontal:1"
    assert points[0].source_id == "cam_frontal"
    assert points[0].pixel_position == Point(509.0, 250.0)
    assert points[0].world_position.x == pytest.approx(50.9)
    assert points[0].world_position.y == pytest.approx(25.0)
    assert points[1].entity_id == "cam_frontal:2"
    assert points[1].world_position.x == pytest.approx(60.0)
    assert points[1].world_position.y == pytest.approx(10.0)


def test_to_trajectory_points_keeps_pixel_position_when_no_calibration():
    """Sem calibração de I3 para a câmera (ex: instalação simples, só
    zonas em pixel), a ingestão não deve falhar: world_position fica
    None, mas pixel_position continua disponível para zonas em pixel."""

    event = TrackEvent(
        camera_id="cam_sem_calibracao",
        session_id="sess_001",
        captured_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
        frame=1,
        tracks=[RawTrack(track_id=1, object_class="person", state="confirmed", u_px=42.0, v_px=7.0, predicted=False)],
        published_at=datetime(2026, 9, 16, tzinfo=timezone.utc),
    )

    points = to_trajectory_points(event, _registry_with_cam_frontal())

    assert len(points) == 1
    assert points[0].world_position is None
    assert points[0].pixel_position == Point(42.0, 7.0)
