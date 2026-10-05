"""Teste de ponta a ponta: parsing de I2/I3 -> projeção -> S1Service ->
mensagem `ods.visao.evento_espacial`, usando os mesmos arquivos de dados
em data/ que o simulate.py consome.

O cenário padrão (data/i2_eventos.json) combina os dois comportamentos
pedidos na demonstração da Sprint 2: um objeto (cam_frontal:1) "dançando"
na borda da zona proibida, que a histerese nunca confirma, e um objeto
(cam_frontal:99, o "Objeto 99" do enunciado) que entra de fato na Zona
Segura — prova de que `zone_id`/`transition` são publicados quando uma
zona é realmente confirmada.
"""

from pathlib import Path

from s1.domain import S1Service, ZoneResolver
from s1.ingestion import (
    CalibrationRegistry,
    load_calibration_events,
    load_track_events,
    load_zones,
    to_trajectory_points,
)
from s1.output import to_spatial_event

DATA_DIR = Path(__file__).parent.parent / "data"


def _run_default_scenario():
    zones = load_zones(DATA_DIR / "definicao_zonas.json")
    service = S1Service(ZoneResolver(zones))  # streaks padrão: 2 p/ entrada, 3 p/ saída

    registry = CalibrationRegistry()
    registry.register_many(load_calibration_events(DATA_DIR / "i3_calibracoes.json"))

    points = []
    for track_event in load_track_events(DATA_DIR / "i2_eventos.json"):
        points.extend(to_trajectory_points(track_event, registry))

    events = [service.process(point) for point in points]
    messages = [to_spatial_event(point, event) for point, event in zip(points, events)]
    return points, events, messages


def test_object_dancing_on_border_never_confirms_zone():
    points, events, messages = _run_default_scenario()

    dancing = [
        (point, event, message)
        for point, event, message in zip(points, events, messages)
        if point.entity_id == "cam_frontal:1"
    ]
    assert len(dancing) == 3
    for _, event, message in dancing:
        assert event.event_type == "none"
        assert event.zone_id is None
        assert "zone_id" not in message["payload"]
        assert "transition" not in message["payload"]


def test_object_99_entering_zone_publishes_zone_id_and_transition():
    points, events, messages = _run_default_scenario()

    entrant = [
        (point, event, message)
        for point, event, message in zip(points, events, messages)
        if point.entity_id == "cam_frontal:99"
    ]
    assert len(entrant) == 2

    _, first_event, first_message = entrant[0]
    assert first_event.event_type == "none"
    assert "zone_id" not in first_message["payload"]
    assert "transition" not in first_message["payload"]

    _, second_event, second_message = entrant[1]
    assert second_event.event_type == "enter"
    assert second_event.zone_id == "zona_segura_01"
    assert second_message["payload"]["zone_id"] == "zona_segura_01"
    assert second_message["payload"]["transition"] == {"type": "entrada"}


def test_all_messages_follow_the_ods_visao_evento_espacial_envelope():
    _, _, messages = _run_default_scenario()
    assert len(messages) == 5
    for message in messages:
        assert message["message_type"] == "event"
        assert message["schema"] == "ods.visao.evento_espacial"
        assert message["schema_version"] == "1.0"
        assert message["producer"] == "S1"
        payload = message["payload"]
        assert set(["entity_id", "source_id", "timestamp", "world_coordinates"]) <= payload.keys()
