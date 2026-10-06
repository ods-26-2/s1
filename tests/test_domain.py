"""Testes do núcleo do S1: geometria, histerese e S1Service (s1/domain.py)."""

from datetime import datetime, timedelta

from s1.domain import HysteresisController, Point, S1Service, TrajectoryPoint, Zone, ZoneResolver

# --------------------------------------------------------------------------
# ZoneResolver (motor de geometria) — zonas em coordenadas de mundo
# --------------------------------------------------------------------------


def _world_square(zone_id, x0, y0, x1, y1, risco="Segura", cor="Verde"):
    return Zone(
        zone_id=zone_id,
        nome_amigavel=zone_id,
        polygon=[Point(x0, y0), Point(x1, y0), Point(x1, y1), Point(x0, y1)],
        risco=risco,
        cor_dashboard=cor,
        coordinate_system="world",
    )


def _resolve_world(resolver, point):
    return resolver.resolve("qualquer_camera", point, None)


def test_resolve_point_inside_single_zone():
    resolver = ZoneResolver([_world_square("a", 0, 0, 10, 10)])
    zone = _resolve_world(resolver, Point(5, 5))
    assert zone is not None
    assert zone.zone_id == "a"


def test_resolve_point_outside_all_zones():
    resolver = ZoneResolver([_world_square("a", 0, 0, 10, 10)])
    assert _resolve_world(resolver, Point(50, 50)) is None


def test_resolve_point_in_gap_between_zones():
    # Réplica do cenário real: zona_segura_01 termina em x=50 e
    # zona_proibida_01 começa em x=51, deixando uma faixa sem zona.
    zone_a = _world_square("zona_segura_01", 0, 0, 50, 50)
    zone_b = _world_square("zona_proibida_01", 51, 0, 100, 50, risco="Critico", cor="Vermelho")
    resolver = ZoneResolver([zone_a, zone_b])
    assert _resolve_world(resolver, Point(50.9, 25)) is None
    zone = _resolve_world(resolver, Point(51.1, 25))
    assert zone.zone_id == "zona_proibida_01"


def test_resolve_overlap_breaks_tie_by_nearest_centroid():
    zone_a = _world_square("a", 0, 0, 10, 10)
    zone_b = _world_square("b", 5, 0, 20, 10)
    resolver = ZoneResolver([zone_a, zone_b])

    # Ponto dentro da sobreposição [5,10]x[0,10], mais próximo do
    # centróide de b (12.5, 5).
    zone = _resolve_world(resolver, Point(9, 5))
    assert zone.zone_id == "b"

    # Mais próximo do centróide de a (5, 5).
    zone = _resolve_world(resolver, Point(6, 5))
    assert zone.zone_id == "a"


# --------------------------------------------------------------------------
# ZoneResolver — zonas em pixel (instalação simples, sem I3)
# --------------------------------------------------------------------------


def _pixel_square(zone_id, camera_id, x0, y0, x1, y1):
    return Zone(
        zone_id=zone_id,
        nome_amigavel=zone_id,
        polygon=[Point(x0, y0), Point(x1, y0), Point(x1, y1), Point(x0, y1)],
        risco="Segura",
        cor_dashboard="Verde",
        coordinate_system="pixel",
        camera_id=camera_id,
    )


def test_pixel_zone_requires_no_world_position():
    zone = _pixel_square("zona_pixel", "cam_unica", 0, 0, 200, 200)
    resolver = ZoneResolver([zone])

    # Sem calibração/I3: só temos a posição em pixel, e mesmo assim a
    # zona em pixel é resolvida normalmente.
    resolved = resolver.resolve("cam_unica", None, Point(100, 100))
    assert resolved is not None
    assert resolved.zone_id == "zona_pixel"


def test_pixel_zone_only_matches_its_own_camera():
    zone = _pixel_square("zona_pixel", "cam_unica", 0, 0, 200, 200)
    resolver = ZoneResolver([zone])

    # Mesmo pixel, mas de outra câmera: não deve casar (a zona em pixel
    # só é válida para a câmera em que foi definida).
    assert resolver.resolve("outra_camera", None, Point(100, 100)) is None


def test_world_zone_is_skipped_when_world_position_is_missing():
    zone = _world_square("zona_mundo", 0, 0, 10, 10)
    resolver = ZoneResolver([zone])

    # Sem world_position (ex: câmera sem calibração), uma zona "world"
    # nunca é candidata, mesmo que a posição em pixel "caberia" nela.
    assert resolver.resolve("cam_sem_calibracao", None, Point(5, 5)) is None


def test_zone_rejects_unknown_coordinate_system():
    try:
        Zone(
            zone_id="z",
            nome_amigavel="z",
            polygon=[Point(0, 0)],
            risco="Segura",
            cor_dashboard="Verde",
            coordinate_system="frame",
        )
    except ValueError:
        return
    raise AssertionError("esperava ValueError para coordinate_system inválido")


def test_pixel_zone_without_camera_id_raises():
    try:
        Zone(
            zone_id="z",
            nome_amigavel="z",
            polygon=[Point(0, 0)],
            risco="Segura",
            cor_dashboard="Verde",
            coordinate_system="pixel",
        )
    except ValueError:
        return
    raise AssertionError("esperava ValueError para zona pixel sem camera_id")


# --------------------------------------------------------------------------
# HysteresisController (controlador de estado espacial)
# --------------------------------------------------------------------------


def test_confirms_entry_only_after_enter_streak():
    hysteresis = HysteresisController(enter_streak=2, exit_streak=2)

    zone, transitioned = hysteresis.update("obj_01", None)
    assert zone is None and not transitioned

    zone, transitioned = hysteresis.update("obj_01", "zona_a")
    assert zone is None and not transitioned  # ainda não atingiu o streak de entrada

    zone, transitioned = hysteresis.update("obj_01", "zona_a")
    assert zone == "zona_a" and transitioned


def test_entry_and_exit_use_independent_streaks():
    """Réplica direta do requisito da arquitetura: "critérios diferentes
    para entrar e para sair". Com enter_streak=2 e exit_streak=4, a
    entrada confirma na 2ª leitura, mas a saída só confirma na 4ª."""

    hysteresis = HysteresisController(enter_streak=2, exit_streak=4)

    hysteresis.update("obj_01", "zona_a")
    zone, transitioned = hysteresis.update("obj_01", "zona_a")
    assert zone == "zona_a" and transitioned  # entrada confirmada em 2 leituras

    for _ in range(3):
        zone, transitioned = hysteresis.update("obj_01", None)
        assert zone == "zona_a" and not transitioned  # ainda dentro: exit_streak não atingido

    zone, transitioned = hysteresis.update("obj_01", None)
    assert zone is None and transitioned  # 4ª leitura fora: saída confirmada


def test_ignores_single_sample_flicker_at_border():
    hysteresis = HysteresisController(enter_streak=2, exit_streak=2)
    sequence = [None, "zona_b", None, "zona_b", None]
    result = [hysteresis.update("obj_01", z)[0] for z in sequence]
    assert all(z is None for z in result)  # oscilação nunca é confirmada


def test_transition_streak_resets_when_raw_zone_changes():
    hysteresis = HysteresisController(enter_streak=2, exit_streak=2)
    hysteresis.update("obj_01", "zona_a")
    hysteresis.update("obj_01", "zona_b")  # muda de candidata antes de confirmar
    zone, transitioned = hysteresis.update("obj_01", "zona_b")
    assert zone == "zona_b" and transitioned


def test_entities_are_tracked_independently():
    hysteresis = HysteresisController(enter_streak=1, exit_streak=1)
    hysteresis.update("obj_01", "zona_a")
    zone, _ = hysteresis.update("obj_02", "zona_b")
    assert hysteresis.current_zone("obj_01") == "zona_a"
    assert zone == "zona_b"


def test_invalid_streaks_raise():
    for kwargs in ({"enter_streak": 0}, {"exit_streak": 0}):
        try:
            HysteresisController(**kwargs)
        except ValueError:
            continue
        raise AssertionError(f"esperava ValueError para {kwargs}")


# --------------------------------------------------------------------------
# S1Service (orquestração)
# --------------------------------------------------------------------------


def _zones():
    return [
        Zone(
            zone_id="zona_segura_01",
            nome_amigavel="Recepção",
            polygon=[Point(0, 0), Point(50, 0), Point(50, 50), Point(0, 50)],
            risco="Segura",
            cor_dashboard="Verde",
            coordinate_system="world",
        ),
        Zone(
            zone_id="zona_proibida_01",
            nome_amigavel="Portão Sul",
            polygon=[Point(51, 0), Point(100, 0), Point(100, 50), Point(51, 50)],
            risco="Critico",
            cor_dashboard="Vermelho",
            coordinate_system="world",
        ),
    ]


def _point(t_ms, x, y, entity="obj_01"):
    return TrajectoryPoint(
        datetime(2026, 1, 1) + timedelta(milliseconds=t_ms), "cam_frontal", entity, Point(x, y)
    )


def test_object_dancing_on_border_never_triggers_zone_event():
    """Réplica do cenário de demonstração da Sprint 2: um objeto
    "dançando" na linha da zona proibida não deve gerar eventos de
    entrada/saída."""

    service = S1Service(ZoneResolver(_zones()), enter_streak=2, exit_streak=2)
    samples = [(0, 50.9), (100, 51.1), (200, 50.8)]

    events = [service.process(_point(t, x, 25.0)) for t, x in samples]

    assert all(event.event_type == "none" for event in events)
    assert all(event.zone_id is None for event in events)
    assert service.occupancy_snapshot() == {"zona_segura_01": 0, "zona_proibida_01": 0}


def test_sustained_entry_triggers_enter_and_updates_occupancy():
    service = S1Service(ZoneResolver(_zones()), enter_streak=2, exit_streak=2)
    samples = [(0, 60.0), (100, 61.0), (200, 62.0)]

    events = [service.process(_point(t, x, 25.0)) for t, x in samples]

    assert events[0].event_type == "none"
    assert events[1].event_type == "enter"
    assert events[1].zone_id == "zona_proibida_01"
    assert events[1].risco == "Critico"
    assert events[2].event_type == "update"
    assert events[2].occupancy == 1
    assert events[2].dwell_seconds == 0.1


def test_exit_after_entry_frees_occupancy():
    service = S1Service(ZoneResolver(_zones()), enter_streak=2, exit_streak=2)
    for t, x in [(0, 60.0), (100, 61.0)]:
        service.process(_point(t, x, 25.0))

    events = [service.process(_point(t, x, 25.0)) for t, x in [(200, 200.0), (300, 200.0)]]

    assert events[-1].event_type == "exit"
    assert events[-1].zone_id is None
    assert events[-1].previous_zone_id == "zona_proibida_01"
    assert service.occupancy_snapshot()["zona_proibida_01"] == 0


def test_two_sources_pointing_to_same_zone_are_counted_as_two_entities():
    service = S1Service(ZoneResolver(_zones()), enter_streak=1, exit_streak=1)
    service.process(_point(0, 25.0, 25.0, entity="obj_01"))
    service.process(_point(0, 25.0, 25.0, entity="obj_02"))

    assert service.occupancy_snapshot()["zona_segura_01"] == 2
