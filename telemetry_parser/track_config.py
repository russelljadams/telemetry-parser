from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional


GLOBAL_MIN_LAP_TIME = 30.0
GLOBAL_MAX_LAP_TIME = 600.0  # 10 minutes — no real lap takes this long

# Porsche 911 GT3 Cup lap time bounds.
# Min: prevents counting formation/warmup laps.
# Max: catches AFK/paused laps (~115-120% of typical median pace).
# NOTE: These are estimates for the 911 Cup Car. Adjust once real data is collected.
# The Cup Car is slower than SFL — expect ~2:20-2:30 at Spa vs ~2:15 in SFL.
DEFAULT_MIN_TIMES: Dict[str, float] = {
    "spa": 135.0,
    "spa 2024 up": 135.0,
    "monza full": 105.0,
    "nurburgring gp": 115.0,
    "barcelona gp": 110.0,
}

DEFAULT_MAX_TIMES: Dict[str, float] = {
    "spa": 146.0,
    "spa 2024 up": 146.0,
    "monza full": 130.0,
    "nurburgring gp": 150.0,
    "barcelona gp": 145.0,
}


@dataclass(frozen=True)
class Zone:
    name: str
    start: float
    end: float


@dataclass(frozen=True)
class TrackConfig:
    track_id: str
    zones: List[Zone]
    min_valid_lap_time: float = 0.0


def get_min_valid_lap_time(track_id: Optional[str]) -> float:
    if track_id and track_id in DEFAULT_MIN_TIMES:
        return DEFAULT_MIN_TIMES[track_id]
    return GLOBAL_MIN_LAP_TIME


def get_max_valid_lap_time(track_id: Optional[str]) -> float:
    if track_id and track_id in DEFAULT_MAX_TIMES:
        return DEFAULT_MAX_TIMES[track_id]
    return GLOBAL_MAX_LAP_TIME


def load_track_config(track_id: Optional[str], base_dir: str = "tracks") -> Optional[TrackConfig]:
    if not track_id:
        return None
    path = Path(base_dir) / f"{track_id}.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    zones = [Zone(**zone) for zone in data.get("zones", [])]
    min_time = data.get("min_valid_lap_time", get_min_valid_lap_time(track_id))
    return TrackConfig(
        track_id=data.get("track_id", track_id),
        zones=zones,
        min_valid_lap_time=min_time,
    )


def tag_zone(lap_dist_pct: float, zones: List[Zone]) -> Optional[str]:
    for zone in zones:
        if zone.start <= zone.end:
            if zone.start <= lap_dist_pct < zone.end:
                return zone.name
        else:
            if lap_dist_pct >= zone.start or lap_dist_pct < zone.end:
                return zone.name
    return None
