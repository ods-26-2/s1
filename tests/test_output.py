from datetime import datetime, timezone

from s1.domain import Point, TrajectoryPoint, ZoneEvent
from s1.output import to_spatial_event


def _point(entity_id="cam_frontal:1", source_id="cam_frontal", x=50.9, y=25.0):
    return TrajectoryPoint(
        timestamp=datetime(2026, 9, 16, 10, 47, 28, 100_000, tzinfo=timezone.utc),
        source_id=source_id,
        entity_id=entity_id,
        world_position=Point(x, y),
    )


def _pixel_only_point(entity_id="cam_frontal:1", source_id="cam_frontal", u=200.0, v=100.0):
    return TrajectoryPoint(
        timestamp=datetime(2026, 9, 16, 10, 47, 28, 100_000, tzinfo=timezone.utc),
        source_id=source_id,
        entity_id=entity_id,
        world_position=None,
        pixel_position=Point(u, v),
    )


def _event(event_type, zone_id=None, previous_zone_id=None, entity_id="cam_frontal:1"):
    return ZoneEvent(
        entity_id=entity_id,
        timestamp=datetime(2026, 9, 16, 10, 47, 28, 100_000, tzinfo=timezone.utc),
        zone_id=zone_id,
        zone_name=None,
        event_type=event_type,
        previous_zone_id=previous_zone_id,
    )


def test_envelope_fields():
    message = to_spatial_event(_point(), _event("none"))
    assert message["message_type"] == "event"
    assert message["schema"] == "ods.visao.evento_espacial"
    assert message["schema_version"] == "1.0"
    assert message["producer"] == "S1"
    assert message["published_at"] == "2026-09-16T10:47:28.100Z"


def test_payload_always_has_required_fields_even_without_zone():
    message = to_spatial_event(_point(), _event("none"))
    payload = message["payload"]
    assert payload["entity_id"] == "cam_frontal:1"
    assert payload["source_id"] == "cam_frontal"
    assert payload["timestamp"] == "2026-09-16T10:47:28.100Z"
    assert payload["world_coordinates"] == {"x": 50.9, "y": 25.0}
    assert "zone_id" not in payload
    assert "transition" not in payload


def test_update_includes_zone_id_but_no_transition():
    message = to_spatial_event(_point(), _event("update", zone_id="zona_segura_01"))
    payload = message["payload"]
    assert payload["zone_id"] == "zona_segura_01"
    assert "transition" not in payload


def test_enter_includes_zone_id_and_entrada_transition():
    message = to_spatial_event(_point(), _event("enter", zone_id="zona_segura_01"))
    payload = message["payload"]
    assert payload["zone_id"] == "zona_segura_01"
    assert payload["transition"] == {"type": "entrada"}


def test_exit_has_no_zone_id_and_saida_transition_with_previous_zone():
    message = to_spatial_event(
        _point(), _event("exit", zone_id=None, previous_zone_id="zona_segura_01")
    )
    payload = message["payload"]
    assert "zone_id" not in payload
    assert payload["transition"] == {"type": "saida", "previous_zone_id": "zona_segura_01"}


def test_transfer_includes_new_zone_id_and_transferencia_transition():
    message = to_spatial_event(
        _point(),
        _event("transfer", zone_id="zona_proibida_01", previous_zone_id="zona_segura_01"),
    )
    payload = message["payload"]
    assert payload["zone_id"] == "zona_proibida_01"
    assert payload["transition"] == {
        "type": "transferencia",
        "previous_zone_id": "zona_segura_01",
    }


def test_published_at_can_be_overridden():
    published_at = datetime(2026, 9, 16, 10, 47, 30, tzinfo=timezone.utc)
    message = to_spatial_event(_point(), _event("none"), published_at=published_at)
    assert message["published_at"] == "2026-09-16T10:47:30.000Z"


def test_world_coordinates_falls_back_to_pixel_when_no_world_position():
    """Instalação em pixel, sem calibração de I3: world_coordinates é
    obrigatório no contrato, então publicamos a posição em pixel."""

    message = to_spatial_event(_pixel_only_point(), _event("none"))
    assert message["payload"]["world_coordinates"] == {"x": 200.0, "y": 100.0}
