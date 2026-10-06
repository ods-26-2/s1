"""
Núcleo do componente S1 - Zonas, ocupação e eventos (Camada 3).

Reúne os modelos de dados, o motor de geometria (associação
ponto -> zona), o controlador de histerese (entrada/saída/permanência)
e o serviço que orquestra os três. 
Isso independe de onde os dados entram (`s1/ingestion.py`) ou de como a saída é publicada (`s1/output.py`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import Polygon as ShapelyPolygon

# --------------------------------------------------------------------------
# Modelos
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Point:
    x: float
    y: float

@dataclass(frozen=True)
class Zone:
    """Artefato de zona nomeada.

    `coordinate_system` declara explicitamente em que sistema o
    `polygon` está expresso — "pixel" (instalação simples, uma câmera,
    sem necessidade de I3) ou "world" (coordenadas de mundo, quando há
    métrica ou mais de uma câmera). Uma zona em pixel só faz sentido
    para uma câmera específica (`camera_id`); uma zona em mundo não
    depende de nenhuma câmera em particular. `calibration_version`
    referencia a versão da transformação de I3 vigente quando a zona
    foi definida (obrigatória para zonas em pixel, já que a geometria
    só é válida para aquela homografia específica).
    """

    zone_id: str
    nome_amigavel: str
    polygon: List[Point]
    risco: str
    cor_dashboard: str
    coordinate_system: str  # "pixel" | "world"
    camera_id: Optional[str] = None
    calibration_version: Optional[str] = None

    def __post_init__(self) -> None:
        if self.coordinate_system not in ("pixel", "world"):
            raise ValueError(
                f"coordinate_system deve ser 'pixel' ou 'world', recebido {self.coordinate_system!r}"
            )
        if self.coordinate_system == "pixel" and not self.camera_id:
            raise ValueError(
                f"zona {self.zone_id!r}: coordinate_system='pixel' exige camera_id"
            )

@dataclass(frozen=True)
class TrajectoryPoint:
    """Uma leitura de trajetória, na(s) representação(ões) disponível(is).

    `pixel_position` vem sempre de I2. `world_position` só existe
    quando havia calibração de I3 vigente para projetar o pixel —
    numa instalação simples (zonas em pixel, sem I3), fica `None`, e
    isso é esperado, não um erro.
    """

    timestamp: datetime
    source_id: str
    entity_id: str
    world_position: Optional[Point] = None
    pixel_position: Optional[Point] = None

@dataclass
class ZoneEvent:
    entity_id: str
    timestamp: datetime
    zone_id: Optional[str]
    zone_name: Optional[str]
    event_type: str  # "enter" | "exit" | "transfer" | "update" | "none"
    entered_at: Optional[datetime] = None
    dwell_seconds: Optional[float] = None
    occupancy: int = 0
    risco: Optional[str] = None
    cor_dashboard: Optional[str] = None
    previous_zone_id: Optional[str] = None

# --------------------------------------------------------------------------
# Motor de Geometria Estática: resolve se um ponto está dentro de um
# polígono (zona), desempatando sobreposições pela zona de centróide
# mais próximo. Cada zona é resolvida no seu próprio sistema de
# coordenadas: zonas "world" usam a posição já projetada para mundo;
# zonas "pixel" usam a posição em pixel bruta e só são candidatas para
# leituras da mesma câmera (`camera_id`) em que foram definidas.
# --------------------------------------------------------------------------

class ZoneResolver:
    def __init__(self, zones: List[Zone]):
        self._zones = list(zones)
        self._polygons: Dict[str, ShapelyPolygon] = {
            zone.zone_id: ShapelyPolygon([(p.x, p.y) for p in zone.polygon])
            for zone in self._zones
        }

    def zones(self) -> List[Zone]:
        return list(self._zones)

    def zone_by_id(self, zone_id: str) -> Optional[Zone]:
        for zone in self._zones:
            if zone.zone_id == zone_id:
                return zone
        return None

    def resolve(
        self,
        source_id: str,
        world_position: Optional[Point],
        pixel_position: Optional[Point],
    ) -> Optional[Zone]:
        """Retorna a zona que contém o ponto, ou None se estiver fora
        de todas as zonas conhecidas (ou se a representação necessária
        para testar uma zona não estiver disponível)."""

        candidates = []
        for zone in self._zones:
            if zone.coordinate_system == "world":
                if world_position is None:
                    continue
                test_point = world_position
            else:  # "pixel"
                if pixel_position is None or zone.camera_id != source_id:
                    continue
                test_point = pixel_position

            shapely_point = ShapelyPoint(test_point.x, test_point.y)
            if self._polygons[zone.zone_id].contains(shapely_point):
                candidates.append((zone, shapely_point))

        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0][0]

        # Sobreposição: desempata pela zona de centróide mais próximo
        # (distância calculada no sistema de coordenadas da própria
        # zona — comparar diretamente pixel com metros não faz sentido,
        # mas esse caso de zonas de sistemas diferentes sobrepostas é
        # uma configuração incomum e fora do escopo desta resolução).
        return min(
            candidates,
            key=lambda pair: self._polygons[pair[0].zone_id].centroid.distance(pair[1]),
        )[0]

# --------------------------------------------------------------------------
# Controlador de Estado Espacial: aplica uma janela de histerese à
# transição de zona de cada entidade, evitando que oscilações na borda
# de uma zona (ex: uma entidade "balançando" na porta) gerem múltiplos
# eventos de entrada/saída. Uma transição só é confirmada depois que a
# nova leitura bruta se repete por N amostras consecutivas — e N é
# diferente para entrar (`enter_streak`) e para sair (`exit_streak`):
# reagir rápido a uma entrada é geralmente desejável, mas confirmar
# saída rápido demais faz a entidade "sumir" por uma oclusão breve ou
# um pé cruzando a linha da zona, por isso o padrão exige mais amostras
# para confirmar saída do que para confirmar entrada.
# --------------------------------------------------------------------------

@dataclass
class _EntityState:
    confirmed_zone_id: Optional[str] = None
    pending_zone_id: Optional[str] = None
    pending_streak: int = 0

class HysteresisController:
    def __init__(self, enter_streak: int = 2, exit_streak: int = 3):
        if enter_streak < 1:
            raise ValueError("enter_streak deve ser >= 1")
        if exit_streak < 1:
            raise ValueError("exit_streak deve ser >= 1")
        self._enter_streak = enter_streak
        self._exit_streak = exit_streak
        self._states: Dict[str, _EntityState] = {}

    def update(
        self, entity_id: str, raw_zone_id: Optional[str]
    ) -> Tuple[Optional[str], bool]:
        """Processa a leitura bruta de zona para a entidade.

        Retorna uma tupla (zona_confirmada, houve_transicao).
        """

        state = self._states.setdefault(entity_id, _EntityState())

        if raw_zone_id == state.confirmed_zone_id:
            # Confirma a permanência: descarta qualquer transição pendente.
            state.pending_zone_id = None
            state.pending_streak = 0
            return state.confirmed_zone_id, False

        if raw_zone_id == state.pending_zone_id:
            state.pending_streak += 1
        else:
            state.pending_zone_id = raw_zone_id
            state.pending_streak = 1

        # raw_zone_id is None => a leitura indica "fora de qualquer
        # zona" (critério de saída); caso contrário, é uma candidata a
        # entrada (numa zona nova ou numa troca direta entre zonas).
        required_streak = self._exit_streak if raw_zone_id is None else self._enter_streak

        if state.pending_streak >= required_streak:
            state.confirmed_zone_id = raw_zone_id
            state.pending_zone_id = None
            state.pending_streak = 0
            return state.confirmed_zone_id, True

        return state.confirmed_zone_id, False

    def current_zone(self, entity_id: str) -> Optional[str]:
        state = self._states.get(entity_id)
        return state.confirmed_zone_id if state else None

    def reset(self, entity_id: str) -> None:
        self._states.pop(entity_id, None)

# --------------------------------------------------------------------------
# S1Service: orquestra geometria + histerese + ocupação. Recebe
# trajetórias contínuas (ver s1/ingestion.py para a conversão a partir
# de I2/I3, que produz tanto a posição em pixel quanto — quando há
# calibração de I3 — a posição projetada em mundo) e emite eventos
# espacialmente qualificados: associação (carimbo) entre a entidade e a
# região, com a transição de bordas protegida pela histerese.
# --------------------------------------------------------------------------

class S1Service:
    def __init__(self, resolver: ZoneResolver, enter_streak: int = 2, exit_streak: int = 3):
        self._resolver = resolver
        self._hysteresis = HysteresisController(enter_streak=enter_streak, exit_streak=exit_streak)
        self._occupancy: Dict[str, Set[str]] = {
            zone.zone_id: set() for zone in resolver.zones()
        }
        self._entered_at: Dict[Tuple[str, str], datetime] = {}

    def process(self, point: TrajectoryPoint) -> ZoneEvent:
        raw_zone = self._resolver.resolve(
            point.source_id, point.world_position, point.pixel_position
        )
        raw_zone_id = raw_zone.zone_id if raw_zone else None

        previous_zone_id = self._hysteresis.current_zone(point.entity_id)
        confirmed_zone_id, transitioned = self._hysteresis.update(
            point.entity_id, raw_zone_id
        )

        event_type = "none"
        if transitioned:
            if previous_zone_id is not None:
                self._occupancy[previous_zone_id].discard(point.entity_id)
                self._entered_at.pop((point.entity_id, previous_zone_id), None)
                event_type = "exit" if confirmed_zone_id is None else "transfer"
            if confirmed_zone_id is not None:
                self._occupancy[confirmed_zone_id].add(point.entity_id)
                self._entered_at[(point.entity_id, confirmed_zone_id)] = point.timestamp
                if previous_zone_id is None:
                    event_type = "enter"
        elif confirmed_zone_id is not None:
            event_type = "update"

        zone = self._resolver.zone_by_id(confirmed_zone_id) if confirmed_zone_id else None
        entered_at = (
            self._entered_at.get((point.entity_id, confirmed_zone_id))
            if confirmed_zone_id
            else None
        )
        dwell_seconds = (
            (point.timestamp - entered_at).total_seconds()
            if entered_at is not None
            else None
        )
        occupancy = (
            len(self._occupancy[confirmed_zone_id]) if confirmed_zone_id else 0
        )

        return ZoneEvent(
            entity_id=point.entity_id,
            timestamp=point.timestamp,
            zone_id=confirmed_zone_id,
            zone_name=zone.nome_amigavel if zone else None,
            event_type=event_type,
            entered_at=entered_at,
            dwell_seconds=dwell_seconds,
            occupancy=occupancy,
            risco=zone.risco if zone else None,
            cor_dashboard=zone.cor_dashboard if zone else None,
            previous_zone_id=previous_zone_id,
        )

    def occupancy_snapshot(self) -> Dict[str, int]:
        """Contagem instantânea de entidades confirmadas por zona."""

        return {zone_id: len(entities) for zone_id, entities in self._occupancy.items()}
