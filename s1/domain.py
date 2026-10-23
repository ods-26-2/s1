"""
Núcleo do componente S1 - Zonas, ocupação e eventos (Camada 3).

Modelos, geometria (zonas e homografia), histerese de entrada/saída e o
serviço que processa os rastros de I2 quadro a quadro, por câmera. Não
sabe de onde as mensagens vêm (`s1/ingestion.py`) nem como são
publicadas (`s1/output.py`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import Polygon as ShapelyPolygon

METERS = "METERS"
PIXELS = "PIXELS"
ENTER = "ENTER"
EXIT = "EXIT"

@dataclass(frozen=True)
class Point:
    x: float
    y: float

@dataclass(frozen=True)
class Zone:
    zone_id: str
    name: str
    polygon: Tuple[Point, ...]
    enter_hysteresis_ms: int
    exit_hysteresis_ms: int
    max_occupancy: Optional[int] = None
    classes: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if len(self.polygon) < 3:
            raise ValueError(f"zona {self.zone_id!r}: polygon precisa de pelo menos 3 pontos")
        if self.enter_hysteresis_ms < 0 or self.exit_hysteresis_ms < 0:
            raise ValueError(f"zona {self.zone_id!r}: histerese não pode ser negativa")
        if self.max_occupancy is not None and self.max_occupancy < 1:
            raise ValueError(f"zona {self.zone_id!r}: max_occupancy deve ser >= 1")

    def accepts(self, object_class: str) -> bool:
        return not self.classes or object_class in self.classes

@dataclass(frozen=True)
class ZonesArtifact:
    artifact_version: str
    coordinates: str
    source_ids: Tuple[str, ...]
    zones: Tuple[Zone, ...]
    space_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.coordinates not in (METERS, PIXELS):
            raise ValueError(f"coordinates deve ser METERS ou PIXELS, recebido {self.coordinates!r}")
        if self.coordinates == METERS and not self.space_id:
            raise ValueError("coordinates=METERS exige space_id")
        if not self.source_ids:
            raise ValueError("source_ids não pode ser vazio")
        if self.coordinates == PIXELS and len(self.source_ids) != 1:
            raise ValueError("coordinates=PIXELS exige exatamente um source_id")
        zone_ids = [zone.zone_id for zone in self.zones]
        if len(set(zone_ids)) != len(zone_ids):
            raise ValueError("zone_id repetido no artefato ZONES")

class DegenerateHomographyError(ValueError):
    """A homografia projeta o ponto para o infinito (w = 0)."""
class Homography:
    def __init__(self, matrix: List[List[float]]):
        if len(matrix) != 3 or any(len(row) != 3 for row in matrix):
            raise ValueError("homografia deve ser uma matriz 3x3")
        self._matrix = matrix

    def project(self, point: Point) -> Point:
        h = self._matrix
        x = h[0][0] * point.x + h[0][1] * point.y + h[0][2]
        y = h[1][0] * point.x + h[1][1] * point.y + h[1][2]
        w = h[2][0] * point.x + h[2][1] * point.y + h[2][2]
        if w == 0:
            raise DegenerateHomographyError(f"homografia degenerada para o pixel {point}")
        return Point(round(x / w, 3), round(y / w, 3))

@dataclass(frozen=True)
class Calibration:
    source_id: str
    artifact_version: str
    valid_from: datetime
    homography: Homography
    space_id: str

class CalibrationRegistry:
    def __init__(self) -> None:
        self._by_source: Dict[str, List[Calibration]] = {}

    def register(self, calibration: Calibration) -> None:
        entries = self._by_source.setdefault(calibration.source_id, [])
        if any(entry.artifact_version == calibration.artifact_version for entry in entries):
            return
        entries.append(calibration)
        entries.sort(key=lambda entry: entry.valid_from)

    def resolve(self, source_id: str, at: datetime) -> Optional[Calibration]:
        current = None
        for calibration in self._by_source.get(source_id, []):
            if calibration.valid_from > at:
                break
            current = calibration
        return current

@dataclass(frozen=True)
class TrackObservation:
    track_id: str
    object_class: str
    point_px: Point
    lost: bool = False

@dataclass(frozen=True)
class Frame:
    source_id: str
    session_id: str
    frame_index: int
    captured_at: datetime
    tracks: Tuple[TrackObservation, ...]
    message_id: Optional[str] = None

@dataclass(frozen=True)
class PositionItem:
    track_id: str
    object_class: str
    point_px: Point
    position_m: Optional[Point]
    space_id: Optional[str]
    zone_ids: Tuple[str, ...]

@dataclass(frozen=True)
class Transition:
    source_id: str
    track_id: str
    object_class: str
    zone_id: str
    kind: str  # ENTER | EXIT
    at: datetime
    entered_at: datetime
    dwell_s: Optional[int] = None

@dataclass(frozen=True)
class Occupancy:
    source_id: str
    session_id: str
    artifact_version: str  # versão do ZONES a que a zona pertence
    zone_id: str
    count: int
    count_by_class: Dict[str, int]
    track_ids: Tuple[str, ...]
    at: datetime
    max_occupancy: Optional[int] = None

@dataclass(frozen=True)
class FrameResult:
    frame: Frame
    artifact_version: str  # versão do ZONES usada no quadro
    positions: Tuple[PositionItem, ...]
    transitions: Tuple[Transition, ...]
    occupancy_changes: Tuple[Occupancy, ...]

class ZoneResolver:
    def __init__(self, artifact: ZonesArtifact):
        self.artifact = artifact
        self._polygons = {
            zone.zone_id: ShapelyPolygon([(p.x, p.y) for p in zone.polygon]) for zone in artifact.zones
        }

    def zones_containing(
        self,
        source_id: str,
        point_px: Point,
        position_m: Optional[Point],
        space_id: Optional[str],
    ) -> List[Zone]:
        artifact = self.artifact
        if source_id not in artifact.source_ids:
            return []
        if artifact.coordinates == METERS:
            if position_m is None or space_id != artifact.space_id:
                return []
            test_point = position_m
        else:
            test_point = point_px
        shapely_point = ShapelyPoint(test_point.x, test_point.y)
        return [zone for zone in artifact.zones if self._polygons[zone.zone_id].contains(shapely_point)]

MembershipKey = Tuple[str, str, str]  # (source_id, track_id, zone_id)

@dataclass
class _Membership:
    inside: bool = False
    pending_since: Optional[datetime] = None

class HysteresisController:
    def __init__(self) -> None:
        self._states: Dict[MembershipKey, _Membership] = {}

    def update(
        self,
        key: MembershipKey,
        raw_inside: bool,
        at: datetime,
        enter_hysteresis_ms: int,
        exit_hysteresis_ms: int,
    ) -> Tuple[bool, Optional[datetime]]:

        state = self._states.get(key)
        if state is None:
            if not raw_inside:
                return False, None
            state = self._states[key] = _Membership()

        if raw_inside == state.inside:
            state.pending_since = None
            if not state.inside:
                del self._states[key]
            return state.inside, None

        if state.pending_since is None:
            state.pending_since = at
        required_ms = enter_hysteresis_ms if raw_inside else exit_hysteresis_ms
        if (at - state.pending_since).total_seconds() * 1000 < required_ms:
            return state.inside, None

        started_at = state.pending_since
        state.inside = raw_inside
        state.pending_since = None
        if not state.inside:
            del self._states[key]
        return raw_inside, started_at

    def close_track(self, source_id: str, track_id: str) -> List[Tuple[str, Optional[datetime]]]:

        closed = []
        for key in [key for key in self._states if key[0] == source_id and key[1] == track_id]:
            state = self._states.pop(key)
            if state.inside:
                closed.append((key[2], state.pending_since))
        return closed

    def reset_source(self, source_id: str) -> None:
        for key in [key for key in self._states if key[0] == source_id]:
            del self._states[key]

class S1Service:
    def __init__(self, zones: ZonesArtifact, calibrations: Optional[CalibrationRegistry] = None):
        self._resolver = ZoneResolver(zones)
        self._pending_zones: Optional[ZonesArtifact] = None
        self._calibrations = calibrations or CalibrationRegistry()
        self._hysteresis = HysteresisController()
        self._sessions: Dict[str, str] = {}
        self._ended_sessions: Set[str] = set()
        self._last_frame_index: Dict[str, int] = {}
        self._members: Dict[Tuple[str, str], Dict[str, str]] = {}  # (source, zone) -> {track: classe}
        self._entered_at: Dict[MembershipKey, datetime] = {}
        self._last_seen: Dict[Tuple[str, str], datetime] = {}

    @property
    def zones(self) -> ZonesArtifact:
        return self._resolver.artifact

    def add_calibration(self, calibration: Calibration) -> None:
        self._calibrations.register(calibration)

    def schedule_zones(self, zones: ZonesArtifact) -> None:
        self._pending_zones = zones

    def start_session(self, session_id: str, sources: List[str], started_at: datetime) -> List[Occupancy]:
        """Reinicia o estado das câmeras que mudaram de sessão. Devolve a
        ocupação zerada das zonas que tinham alguém (sem publicar saídas)."""

        changed = [source for source in sources if self._sessions.get(source) != session_id]
        if not changed:
            return []
        is_change = any(self._sessions.get(source) is not None for source in changed)
        apply_zones = self._pending_zones is not None and (is_change or not self._sessions)
        to_reset = list(dict.fromkeys(changed + (list(self._sessions) if apply_zones else [])))
        for source in changed:
            previous = self._sessions.get(source)
            if previous is not None:
                self._ended_sessions.add(previous)
            self._sessions[source] = session_id
            self._last_frame_index.pop(source, None)
        reset: List[Occupancy] = []
        for source in to_reset:
            reset.extend(self._reset_source(source, started_at))
        if apply_zones:
            self._resolver = ZoneResolver(self._pending_zones)
            self._pending_zones = None
        return reset

    def process_frame(self, frame: Frame) -> Optional[FrameResult]:

        source = frame.source_id
        if frame.session_id in self._ended_sessions:
            return None
        reset = []
        if self._sessions.get(source) != frame.session_id:
            reset = self.start_session(frame.session_id, [source], frame.captured_at)
        last_index = self._last_frame_index.get(source)
        if last_index is not None and frame.frame_index <= last_index:
            return None
        self._last_frame_index[source] = frame.frame_index

        positions: List[PositionItem] = []
        exits: List[Transition] = []
        enters: List[Transition] = []
        for track in frame.tracks:
            if track.lost:
                exits.extend(self._close_track(source, track, frame.captured_at))
                continue
            positions.append(self._observe(frame, track, exits, enters))

        transitions = exits + enters
        occupancy_changes = {(occupancy.source_id, occupancy.zone_id): occupancy for occupancy in reset}
        for zone_id in dict.fromkeys(transition.zone_id for transition in transitions):
            occupancy_changes[(source, zone_id)] = self._occupancy(source, zone_id, frame.captured_at)
        return FrameResult(
            frame=frame,
            artifact_version=self.zones.artifact_version,
            positions=tuple(positions),
            transitions=tuple(transitions),
            occupancy_changes=tuple(occupancy_changes.values()),
        )

    def occupancy_snapshot(self, source_id: str) -> Dict[str, int]:
        return {
            zone.zone_id: len(self._members.get((source_id, zone.zone_id), {})) for zone in self.zones.zones
        }

    def _observe(
        self, frame: Frame, track: TrackObservation, exits: List[Transition], enters: List[Transition]
    ) -> PositionItem:
        source, at = frame.source_id, frame.captured_at
        self._last_seen[(source, track.track_id)] = at

        position_m, space_id = None, None
        calibration = self._calibrations.resolve(source, at)
        if calibration is not None:
            position_m, space_id = calibration.homography.project(track.point_px), calibration.space_id

        candidates = self._resolver.zones_containing(source, track.point_px, position_m, space_id)
        candidate_ids = {zone.zone_id for zone in candidates}
        for zone in self.zones.zones:
            key = (source, track.track_id, zone.zone_id)
            raw_inside = zone.zone_id in candidate_ids and zone.accepts(track.object_class)
            inside, started_at = self._hysteresis.update(
                key, raw_inside, at, zone.enter_hysteresis_ms, zone.exit_hysteresis_ms
            )
            if started_at is None:
                continue
            if inside:
                self._members.setdefault((source, zone.zone_id), {})[track.track_id] = track.object_class
                self._entered_at[key] = started_at
                enters.append(Transition(source, track.track_id, track.object_class, zone.zone_id, ENTER, started_at, started_at))
            else:
                exits.append(self._exit(source, track.track_id, track.object_class, zone.zone_id, started_at))

        return PositionItem(
            track_id=track.track_id,
            object_class=track.object_class,
            point_px=track.point_px,
            position_m=position_m,
            space_id=space_id,
            zone_ids=tuple(zone.zone_id for zone in candidates),
        )

    def _close_track(self, source: str, track: TrackObservation, at: datetime) -> List[Transition]:
        """Rastro LOST: sai de toda zona em que estava. A saída aconteceu no
        início da saída pendente, se havia uma; senão, na última vez em que
        o rastro foi visto."""

        last_seen = self._last_seen.pop((source, track.track_id), at)
        return [
            self._exit(source, track.track_id, track.object_class, zone_id, pending_exit or last_seen)
            for zone_id, pending_exit in self._hysteresis.close_track(source, track.track_id)
        ]

    def _exit(self, source: str, track_id: str, object_class: str, zone_id: str, at: datetime) -> Transition:
        self._members.get((source, zone_id), {}).pop(track_id, None)
        entered_at = self._entered_at.pop((source, track_id, zone_id))
        dwell_s = int(round((at - entered_at).total_seconds()))
        return Transition(source, track_id, object_class, zone_id, EXIT, at, entered_at, dwell_s)

    def _occupancy(self, source: str, zone_id: str, at: datetime) -> Occupancy:
        zone = next(zone for zone in self.zones.zones if zone.zone_id == zone_id)
        members = self._members.get((source, zone_id), {})
        count_by_class = {object_class: 0 for object_class in zone.classes}
        for object_class in members.values():
            count_by_class[object_class] = count_by_class.get(object_class, 0) + 1
        return Occupancy(
            source_id=source,
            session_id=self._sessions[source],
            artifact_version=self.zones.artifact_version,
            zone_id=zone_id,
            count=len(members),
            count_by_class=count_by_class,
            track_ids=tuple(sorted(members)),
            at=at,
            max_occupancy=zone.max_occupancy,
        )

    def _reset_source(self, source: str, at: datetime) -> List[Occupancy]:
        occupied = [zone_id for (s, zone_id), members in self._members.items() if s == source and members]
        self._hysteresis.reset_source(source)
        for key in [key for key in self._members if key[0] == source]:
            del self._members[key]
        for key in [key for key in self._entered_at if key[0] == source]:
            del self._entered_at[key]
        for key in [key for key in self._last_seen if key[0] == source]:
            del self._last_seen[key]
        return [self._occupancy(source, zone_id, at) for zone_id in occupied]
