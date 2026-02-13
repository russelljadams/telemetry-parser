from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from .segments import LapSegment
from .track_config import Zone


@dataclass(frozen=True)
class SectorTime:
    lap_number: int
    sector_name: str
    sector_time: float


def compute_sector_times(
    session_time: Sequence[float],
    lap_dist_pct: Sequence[float],
    segments: Sequence[LapSegment],
    zones: Sequence[Zone],
) -> List[Dict]:
    """Compute sector times by interpolating SessionTime at zone boundary crossings.

    Returns a list of dicts with keys: lap_number, sector_name, sector_time.
    """
    if not zones or not segments:
        return []

    # Collect zone boundaries (excluding 0.0 and 1.0)
    boundaries = sorted({z.start for z in zones} | {z.end for z in zones})
    boundaries = [b for b in boundaries if 0.0 < b < 1.0]

    results: List[Dict] = []

    for seg in segments:
        if not seg.is_complete:
            continue

        start_idx = seg.start_idx
        end_idx = seg.end_idx
        if end_idx <= start_idx:
            continue

        # Find times at each boundary crossing within this lap
        crossing_times: List[float] = [float(session_time[start_idx])]

        for boundary in boundaries:
            crossing_time = _find_boundary_crossing(
                session_time, lap_dist_pct, start_idx, end_idx, boundary,
            )
            if crossing_time is not None:
                crossing_times.append(crossing_time)

        crossing_times.append(float(session_time[end_idx]))

        # Generate sector times from consecutive crossing pairs
        if len(crossing_times) != len(boundaries) + 2:
            continue

        for i, zone in enumerate(zones):
            if i + 1 < len(crossing_times):
                sector_time = crossing_times[i + 1] - crossing_times[i]
                results.append({
                    "lap_number": seg.lap_number,
                    "sector_name": zone.name,
                    "sector_time": round(sector_time, 4),
                })

    return results


def _find_boundary_crossing(
    session_time: Sequence[float],
    lap_dist_pct: Sequence[float],
    start_idx: int,
    end_idx: int,
    boundary: float,
) -> float | None:
    """Find the interpolated time when lap_dist_pct crosses a boundary value."""
    for i in range(start_idx, end_idx):
        pct_before = float(lap_dist_pct[i])
        pct_after = float(lap_dist_pct[i + 1])

        # Normal crossing (not a reset/wrap)
        if pct_before < boundary <= pct_after and (pct_after - pct_before) < 0.5:
            if pct_after == pct_before:
                return float(session_time[i])
            frac = (boundary - pct_before) / (pct_after - pct_before)
            t = float(session_time[i]) + frac * (float(session_time[i + 1]) - float(session_time[i]))
            return t

    return None
