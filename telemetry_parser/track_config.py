from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Dict, List, Optional


@dataclass(frozen=True)
class Zone:
    name: str
    start: float
    end: float


@dataclass(frozen=True)
class TrackConfig:
    track_id: str
    zones: List[Zone]


def load_track_config(track_id: Optional[str], base_dir: str = "tracks") -> Optional[TrackConfig]:
    if not track_id:
        return None
    path = Path(base_dir) / f"{track_id}.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    zones = [Zone(**zone) for zone in data.get("zones", [])]
    return TrackConfig(track_id=data.get("track_id", track_id), zones=zones)


def tag_zone(lap_dist_pct: float, zones: List[Zone]) -> Optional[str]:
    for zone in zones:
        if zone.start <= zone.end:
            if zone.start <= lap_dist_pct < zone.end:
                return zone.name
        else:
            if lap_dist_pct >= zone.start or lap_dist_pct < zone.end:
                return zone.name
    return None
