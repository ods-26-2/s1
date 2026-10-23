"""Testes do núcleo do S1 (s1/domain.py): artefato ZONES, geometria,
calibração, histerese e S1Service quadro a quadro."""

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from s1.domain import (
    ENTER,
    EXIT,
    METERS,
    PIXELS,
    Calibration,
    CalibrationRegistry,
    Homography,
    HysteresisController,
    Point,
    S1Service,
    ZonesArtifact,
)
from tests.factories import SESSION, artifact, at, frame, square, track

NEW_SESSION = "22222222-2222-4222-8222-222222222222"

def _calibrations(*sources, space_id="patio-1", scale=0.01):
    registry = CalibrationRegistry()
    for source in sources:
        registry.register(
            Calibration(
                source_id=source,
                artifact_version=f"cal-{source}-1",
                valid_from=datetime(2026, 9, 1, tzinfo=timezone.utc),
                homography=Homography([[scale, 0, 0], [0, scale, 0], [0, 0, 1]]),
                space_id=space_id,
            )
        )
    return registry

def _events(result):
    return [(t.kind, t.track_id, t.zone_id) for t in result.transitions]

@pytest.mark.parametrize(
    "kwargs",
    [
        {"coordinates": "PIXEL"},  # o enum do contrato é PIXELS
        {"coordinates": PIXELS, "source_ids": ("cam0", "cam1")},  # PIXELS: exatamente uma câmera
        {"coordinates": METERS, "space_id": None},
        {"source_ids": ()},
        {"zones": (square("a", 0, 0, 1, 1), square("a", 2, 2, 3, 3))},
    ],
)
def test_zones_artifact_rejects_values_outside_the_contract(kwargs):
    base = dict(artifact_version="v", coordinates=PIXELS, source_ids=("cam0",), zones=(), space_id=None)
    base.update(kwargs)
    with pytest.raises(ValueError):
        ZonesArtifact(**base)

@pytest.mark.parametrize(
    "kwargs",
    [{"polygon": (Point(0, 0), Point(1, 0))}, {"enter_hysteresis_ms": -1}, {"max_occupancy": 0}],
)
def test_zone_rejects_invalid_values(kwargs):
    with pytest.raises(ValueError):
        replace(square("z", 0, 0, 1, 1), **kwargs)

def test_zone_classes_filter():
    assert square("a", 0, 0, 1, 1).accepts("person")
    assert not square("a", 0, 0, 1, 1, classes=("vehicle",)).accepts("person")

def test_homography_projects_raw_pixel_to_meters():
    assert Homography([[0.01, 0, 0], [0, 0.01, 0], [0, 0, 1]]).project(Point(505, 702)) == Point(5.05, 7.02)

def test_calibration_registry_picks_version_valid_at_capture_time():
    registry = CalibrationRegistry()
    for version, day in (("cal-cam0-1", 1), ("cal-cam0-2", 20)):
        registry.register(
            Calibration("cam0", version, datetime(2026, 9, day, tzinfo=timezone.utc), Homography([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), "patio-1")
        )
    assert registry.resolve("cam0", datetime(2026, 9, 10, tzinfo=timezone.utc)).artifact_version == "cal-cam0-1"
    assert registry.resolve("cam0", datetime(2026, 9, 25, tzinfo=timezone.utc)).artifact_version == "cal-cam0-2"
    assert registry.resolve("cam0", datetime(2026, 8, 1, tzinfo=timezone.utc)) is None
    assert registry.resolve("cam9", datetime(2026, 9, 25, tzinfo=timezone.utc)) is None

def _feed(readings, enter_ms=500, exit_ms=500):
    hysteresis = HysteresisController()
    return [hysteresis.update(("cam0", "t-1", "z"), inside, at(t), enter_ms, exit_ms) for t, inside in readings]

def test_confirms_entry_after_enter_hysteresis_and_reports_when_it_started():
    assert _feed([(0, True), (300, True), (500, True)]) == [(False, None), (False, None), (True, at(0))]

def test_entry_and_exit_use_different_durations():
    results = _feed([(0, True), (500, True), (1000, False), (2000, False), (2500, False)], 500, 1500)
    assert results[1] == (True, at(0))
    assert results[2:4] == [(True, None), (True, None)]
    assert results[4] == (False, at(1000))

def test_flicker_at_border_is_never_confirmed():
    results = _feed([(0, False), (100, True), (200, False), (300, True), (400, False)], enter_ms=150)
    assert all(inside is False for inside, _ in results)

def test_position_has_one_item_per_confirmed_track_and_empty_frames_are_kept():
    service = S1Service(artifact(square("z", 0, 0, 100, 100)))
    empty = service.process_frame(frame(0))
    result = service.process_frame(frame(1, track("t-1", 10, 10), track("t-2", 500, 500), track("t-3", 10, 10, lost=True)))

    assert empty.positions == ()
    assert [(p.track_id, p.zone_ids) for p in result.positions] == [("t-1", ("z",)), ("t-2", ())]

def test_position_in_meters_only_when_the_camera_has_calibration():
    zones = artifact(square("vaga-12", 3, 6.5, 5.5, 11.5), coordinates=METERS, source_ids=("cam0", "cam1"), space_id="patio-1")
    service = S1Service(zones, _calibrations("cam0"))

    calibrated = service.process_frame(frame(0, track("t-1", 400, 900), source="cam0")).positions[0]
    uncalibrated = service.process_frame(frame(0, track("t-2", 400, 900), source="cam1")).positions[0]

    assert (calibrated.position_m, calibrated.space_id, calibrated.zone_ids) == (Point(4.0, 9.0), "patio-1", ("vaga-12",))
    assert (uncalibrated.position_m, uncalibrated.space_id, uncalibrated.zone_ids) == (None, None, ())

def test_meters_ignores_calibration_of_another_space():
    zones = artifact(square("a", 0, 0, 10, 10), coordinates=METERS, space_id="patio-1")
    service = S1Service(zones, _calibrations("cam0", space_id="outro-espaco"))
    assert service.process_frame(frame(0, track("t-1", 500, 500))).positions[0].zone_ids == ()

def test_overlapping_zones_are_all_reported_without_tie_break():
    service = S1Service(artifact(square("b", 50, 0, 200, 100), square("a", 0, 0, 100, 100)))
    assert service.process_frame(frame(0, track("t-1", 90, 50))).positions[0].zone_ids == ("b", "a")

def test_ignores_cameras_outside_source_ids():
    service = S1Service(artifact(square("z", 0, 0, 100, 100, enter_ms=0)))
    result = service.process_frame(frame(0, track("t-1", 10, 10), source="cam9"))
    assert result.positions[0].zone_ids == () and result.transitions == ()

def _demo_service():
    return S1Service(
        artifact(
            square("zona_segura_01", 0, 0, 500, 500, 100, 300, max_occupancy=20, classes=("person",)),
            square("zona_proibida_01", 510, 0, 1000, 500, 100, 400, max_occupancy=5, classes=("person",)),
        )
    )

def test_object_dancing_on_border_never_triggers_transition():
    service = _demo_service()
    results = [service.process_frame(frame(i, track("t-1", x, 250))) for i, x in enumerate((509, 511, 508))]
    assert all(result.transitions == () for result in results)

def test_enter_is_stamped_with_the_crossing_instant_and_updates_occupancy():
    service = _demo_service()
    service.process_frame(frame(4, track("t-99", 100, 100)))
    result = service.process_frame(frame(5, track("t-99", 150, 100)))

    (transition,) = result.transitions
    assert (transition.kind, transition.zone_id, transition.at, transition.entered_at, transition.dwell_s) == (
        ENTER, "zona_segura_01", at(400), at(400), None,
    )
    (occupancy,) = result.occupancy_changes
    assert (occupancy.count, occupancy.count_by_class, occupancy.track_ids, occupancy.max_occupancy) == (
        1, {"person": 1}, ("t-99",), 20,
    )

def test_exit_reports_integer_dwell_between_crossings():
    service = S1Service(artifact(square("z", 0, 0, 100, 100, 100, 200)))
    for index, ms in ((0, 0), (1, 100), (2, 61_000)):
        service.process_frame(frame(index, track("t-1", 50, 50), ms=ms))
    service.process_frame(frame(3, track("t-1", 500, 500), ms=61_200))
    result = service.process_frame(frame(4, track("t-1", 500, 500), ms=61_400))

    (transition,) = result.transitions
    assert (transition.kind, transition.at, transition.dwell_s) == (EXIT, at(61_200), 61)
    assert isinstance(transition.dwell_s, int)
    assert result.occupancy_changes[0].count == 0

def test_lost_track_exits_at_last_sighting_and_frees_occupancy():
    service = _demo_service()
    service.process_frame(frame(4, track("t-99", 100, 100)))
    service.process_frame(frame(5, track("t-99", 150, 100)))
    service.process_frame(frame(40, track("t-99", 200, 120)))  # último quadro em que foi visto
    result = service.process_frame(frame(41, track("t-99", 200, 120, lost=True)))

    assert result.positions == ()
    (transition,) = result.transitions
    assert (transition.kind, transition.at, transition.dwell_s) == (EXIT, at(4000), 4)
    assert result.occupancy_changes[0].count == 0
    assert service.occupancy_snapshot("cam0")["zona_segura_01"] == 0

def test_lost_track_with_pending_exit_exits_when_it_left():
    service = S1Service(artifact(square("z", 0, 0, 100, 100, 0, 1000)))
    service.process_frame(frame(0, track("t-1", 50, 50)))
    service.process_frame(frame(1, track("t-1", 500, 500)))  # saiu em t=100, saída ainda pendente
    result = service.process_frame(frame(2, track("t-1", 500, 500, lost=True)))
    assert result.transitions[0].at == at(100)

def test_lost_track_never_confirmed_produces_nothing():
    service = _demo_service()
    service.process_frame(frame(0, track("t-1", 511, 250)))
    result = service.process_frame(frame(1, track("t-1", 511, 250, lost=True)))
    assert result.transitions == () and result.occupancy_changes == ()

def test_occupancy_is_per_camera():
    """Duas câmeras vendo a mesma pessoa: cada uma conta 1 na sua
    ocupação; quem funde as câmeras é S2."""

    zones = artifact(square("vaga-12", 3, 6.5, 5.5, 11.5, 0, 0), coordinates=METERS, source_ids=("cam0", "cam1"), space_id="patio-1")
    service = S1Service(zones, _calibrations("cam0", "cam1"))
    first = service.process_frame(frame(0, track("t-1", 400, 900), source="cam0"))
    second = service.process_frame(frame(0, track("t-7", 410, 905), source="cam1"))

    assert [(o.source_id, o.count) for o in first.occupancy_changes] == [("cam0", 1)]
    assert [(o.source_id, o.count) for o in second.occupancy_changes] == [("cam1", 1)]

def test_nested_zones_count_the_track_in_both():
    service = S1Service(artifact(square("galpao", 0, 0, 1000, 1000), square("mesa_4", 400, 400, 600, 600)))
    results = [service.process_frame(frame(i, track("t-1", xy, xy))) for i, xy in enumerate((100, 100, 500, 500, 100, 100))]

    assert _events(results[1]) == [(ENTER, "t-1", "galpao")]
    assert _events(results[3]) == [(ENTER, "t-1", "mesa_4")]
    assert _events(results[5]) == [(EXIT, "t-1", "mesa_4")]
    assert service.occupancy_snapshot("cam0") == {"galpao": 1, "mesa_4": 0}

def test_direct_move_respects_exit_hysteresis_of_the_zone_left():
    service = S1Service(artifact(square("A", 0, 0, 100, 100, 0, 500), square("B", 100, 0, 200, 100, 100, 100)))
    service.process_frame(frame(0, track("t-1", 50, 50)))
    results = {i: service.process_frame(frame(i, track("t-1", 150, 50))) for i in (1, 2, 4, 6)}

    assert _events(results[2]) == [(ENTER, "t-1", "B")]
    assert _events(results[4]) == []
    assert _events(results[6]) == [(EXIT, "t-1", "A")]

def test_exits_come_before_enters_in_the_same_frame():
    service = S1Service(artifact(square("B", 100, 0, 200, 100, 0, 0), square("A", 0, 0, 100, 100, 0, 0)))
    service.process_frame(frame(0, track("t-1", 50, 50)))
    assert _events(service.process_frame(frame(1, track("t-1", 150, 50)))) == [(EXIT, "t-1", "A"), (ENTER, "t-1", "B")]

def test_zone_only_counts_its_classes_but_position_reports_geometry():
    service = S1Service(artifact(square("vaga-12", 0, 0, 100, 100, 0, 0, max_occupancy=1, classes=("vehicle",))))
    result = service.process_frame(frame(0, track("p-1", 50, 50, "person"), track("c-1", 50, 50, "vehicle")))

    assert [p.zone_ids for p in result.positions] == [("vaga-12",), ("vaga-12",)]
    assert _events(result) == [(ENTER, "c-1", "vaga-12")]
    assert result.occupancy_changes[0].count_by_class == {"vehicle": 1}

def test_new_session_resets_state_without_fake_exits():
    """Replay com SEEK/LOOP: o tempo de captura volta. O estado da sessão
    anterior é descartado — sem saídas inventadas — e a ocupação zera."""

    service = S1Service(artifact(square("z", 0, 0, 100, 100, 0, 0)))
    for index, ms in ((0, 0), (1, 60_000)):
        service.process_frame(frame(index, track("t-1", 50, 50), ms=ms))

    reset = service.start_session(NEW_SESSION, ["cam0"], at(0))
    assert [(o.zone_id, o.count, o.session_id) for o in reset] == [("z", 0, NEW_SESSION)]

    result = service.process_frame(frame(0, track("t-1", 500, 500), ms=0, session=NEW_SESSION))
    assert result.transitions == ()

def test_start_session_is_idempotent():
    service = S1Service(artifact(square("z", 0, 0, 100, 100, 0, 0)))
    service.process_frame(frame(0, track("t-1", 50, 50)))
    assert service.start_session(NEW_SESSION, ["cam0"], at(0)) != []
    assert service.start_session(NEW_SESSION, ["cam0"], at(0)) == []

def test_frames_of_an_ended_session_are_ignored():
    service = S1Service(artifact(square("z", 0, 0, 100, 100)))
    service.process_frame(frame(0))
    service.start_session(NEW_SESSION, ["cam0"], at(0))
    assert service.process_frame(frame(1, session=SESSION)) is None

def test_frame_from_unannounced_session_resets_the_camera():
    service = S1Service(artifact(square("z", 0, 0, 100, 100, 0, 0)))
    service.process_frame(frame(0, track("t-1", 50, 50)))
    result = service.process_frame(frame(0, session=NEW_SESSION))
    assert [(o.count, o.session_id) for o in result.occupancy_changes] == [(0, NEW_SESSION)]

def test_repeated_or_out_of_order_frames_are_ignored():
    service = S1Service(artifact(square("z", 0, 0, 100, 100)))
    assert service.process_frame(frame(5)) is not None
    assert service.process_frame(frame(5)) is None
    assert service.process_frame(frame(4)) is None

def test_new_zones_take_effect_at_next_session():
    service = S1Service(artifact(square("z", 0, 0, 100, 100), version="zones-t-1"))
    service.schedule_zones(artifact(square("z2", 0, 0, 100, 100), version="zones-t-2"))
    assert service.zones.artifact_version == "zones-t-1"

    service.start_session(NEW_SESSION, ["cam0"], at(0))
    assert service.zones.artifact_version == "zones-t-2"
