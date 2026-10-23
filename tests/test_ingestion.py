"""Testes da entrada do S1 (s1/ingestion.py): contratos consumidos e
artefatos ZONES/CALIBRATION."""

import hashlib
import json
from pathlib import Path

import pytest

from s1.domain import Point
from s1.ingestion import (
    ArtifactRegistry,
    ContractError,
    load_json,
    parse_artifact_published,
    parse_calibration,
    parse_session_started,
    parse_tracks,
    parse_zones,
)
from tests.factories import SESSION, at, calibration_message, raw_track, session_message, tracks_message

DATA_DIR = Path(__file__).parent.parent / "data"

def test_parse_tracks_reads_envelope_and_payload():
    message = tracks_message(48213, [raw_track("t-0391", 505, 702), raw_track("t-0400", 10, 20, state="LOST")])
    frame = parse_tracks(message)

    assert (frame.source_id, frame.session_id, frame.frame_index) == ("cam0", SESSION, 48213)
    assert frame.captured_at == at(4_821_300)
    assert frame.message_id == message["message_id"]
    first, second = frame.tracks
    assert (first.track_id, first.object_class, first.point_px, first.lost) == ("t-0391", "person", Point(505, 702), False)
    assert second.lost is True

def test_parse_tracks_accepts_empty_frame():
    assert parse_tracks(tracks_message(0, [])).tracks == ()

@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: m.update(schema="ods.inferencia.rastreio"),
        lambda m: m.pop("session_id"),
        lambda m: m["payload"].pop("frame_index"),
        lambda m: m["payload"]["tracks"][0].update(state="TENTATIVE"),
        lambda m: m["payload"]["tracks"][0].update(point_px=[1]),
    ],
    ids=["schema-antigo", "sem-session_id", "sem-frame_index", "state-fora-do-enum", "point_px-malformado"],
)
def test_parse_tracks_rejects_messages_outside_the_contract(mutate):
    message = tracks_message(0, [raw_track("t-1", 1, 2)])
    mutate(message)
    with pytest.raises(ContractError):
        parse_tracks(message)

def test_parse_calibration_takes_source_and_version_from_envelope():
    calibration = parse_calibration(calibration_message(source="cam0", version="cal-cam0-3", space_id="quadra-1"))

    assert (calibration.source_id, calibration.artifact_version, calibration.space_id) == ("cam0", "cal-cam0-3", "quadra-1")
    assert calibration.homography.project(Point(500, 700)) == Point(5.0, 7.0)

def test_parse_calibration_rejects_missing_homography():
    message = calibration_message()
    del message["payload"]["homography_px_to_m"]
    with pytest.raises(ContractError):
        parse_calibration(message)

def test_parse_session_started():
    session = parse_session_started(session_message("s-2", sources=("cam0", "mic0"), ms=1000))
    assert (session.session_id, session.started_at, session.sources) == ("s-2", at(1000), ("cam0", "mic0"))

def test_parse_artifact_published():
    descriptor = parse_artifact_published(
        {
            "schema": "ods.artifact.published",
            "message_type": "event",
            "producer": "A8",
            "payload": {
                "artifact_id": "zones-a8", "artifact_version": "zones-a8-2", "kind": "ZONES", "owner": "A8",
                "uri": "registry://a8/zones/2", "hash_sha256": "abc", "valid_from": "2026-09-23T14:00:00.000Z",
                "schema_version": "1.0",
            },
        }
    )
    assert (descriptor.artifact_id, descriptor.artifact_version, descriptor.kind) == ("zones-a8", "zones-a8-2", "ZONES")

def _zones(**overrides):
    content = load_json(DATA_DIR / "zones_exemplo_a8.json")
    content.update(overrides)
    return content

def _zone(**overrides):
    zone = dict(_zones()["zones"][0])
    zone.update(overrides)
    return zone

def test_parse_zones_reads_the_a8_example_from_the_specification():
    zones = parse_zones(_zones(), "zones-a8-2")

    assert (zones.artifact_version, zones.space_id, zones.coordinates, zones.source_ids) == (
        "zones-a8-2", "patio-1", "METERS", ("cam0", "cam1"),
    )
    (zone,) = zones.zones
    assert (zone.zone_id, zone.name, zone.polygon[0]) == ("vaga-12", "Vaga 12", Point(3.0, 6.5))
    assert (zone.enter_hysteresis_ms, zone.exit_hysteresis_ms, zone.max_occupancy, zone.classes) == (500, 1500, 1, ("vehicle",))

def test_parse_zones_reads_the_a3_pixel_demo():
    zones = parse_zones(load_json(DATA_DIR / "zones_a3.json"), "zones-a3-1")
    assert (zones.coordinates, zones.source_ids, zones.space_id) == ("PIXELS", ("cam_frontal",), None)

def test_optional_zone_fields_default_to_no_limit_and_all_classes():
    zone = _zone()
    del zone["max_occupancy"], zone["classes"]
    parsed = parse_zones(_zones(zones=[zone]), "v").zones[0]
    assert (parsed.max_occupancy, parsed.classes) == (None, ())

@pytest.mark.parametrize(
    "content",
    [
        _zones(coordinates="PIXEL"),
        _zones(coordinates="PIXELS"),  # PIXELS com duas câmeras
        _zones(space_id=None),
        _zones(source_ids=[]),
        {k: v for k, v in _zones().items() if k != "zones"},
        _zones(zones=[_zone(name=None)]),
        _zones(zones=[_zone(polygon=[[0, 0], [1, 0]])]),
        _zones(zones=[_zone(polygon=[[0, 0, 0], [1, 0], [1, 1]])]),
        _zones(zones=[_zone(enter_hysteresis_ms=None)]),
        _zones(zones=[_zone(exit_hysteresis_ms=1.5)]),
        _zones(zones=[_zone(), _zone()]),
    ],
    ids=[
        "PIXEL-no-singular", "PIXELS-com-duas-cameras", "METERS-sem-space_id", "source_ids-vazio", "sem-zones",
        "zona-sem-name", "poligono-com-2-pontos", "ponto-malformado", "sem-enter_hysteresis_ms",
        "histerese-nao-inteira", "zone_id-repetido",
    ],
)
def test_parse_zones_rejects_artifacts_outside_the_contract(content):
    with pytest.raises(ContractError):
        parse_zones(content, "v")

def _descriptor(uri, content_bytes):
    return parse_artifact_published(
        {
            "schema": "ods.artifact.published",
            "payload": {
                "artifact_id": "zones-a8", "artifact_version": "zones-a8-2", "kind": "ZONES", "owner": "A8",
                "uri": uri, "hash_sha256": hashlib.sha256(content_bytes).hexdigest(),
                "valid_from": "2026-09-23T14:00:00.000Z", "schema_version": "1.0",
            },
        }
    )

def test_registry_loads_artifact_by_uri_when_hash_matches(tmp_path):
    data = json.dumps(_zones()).encode()
    (tmp_path / "a8" / "zones").mkdir(parents=True)
    (tmp_path / "a8" / "zones" / "2").write_bytes(data)

    assert ArtifactRegistry(tmp_path).load(_descriptor("registry://a8/zones/2", data)) == _zones()

def test_registry_rejects_content_with_wrong_hash(tmp_path):
    (tmp_path / "zones").write_bytes(b'{"adulterado": true}')
    with pytest.raises(ContractError):
        ArtifactRegistry(tmp_path).load(_descriptor("registry://zones", b"{}"))
