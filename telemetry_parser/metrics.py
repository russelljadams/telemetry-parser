from __future__ import annotations

from dataclasses import dataclass
from statistics import median, pstdev
from typing import Dict, Iterable, List, Sequence

from .segments import LapSegment


@dataclass(frozen=True)
class LapMetrics:
    lap_count: int
    complete_lap_count: int
    best_lap: float
    median_lap: float
    worst_lap: float
    stddev_lap: float
    iqr_lap: float


def _percentile(sorted_vals: Sequence[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def compute_lap_metrics(segments: Sequence[LapSegment]) -> LapMetrics:
    lap_times = [seg.lap_time for seg in segments if seg.is_complete and seg.lap_time > 0]
    lap_times_sorted = sorted(lap_times)

    if not lap_times_sorted:
        return LapMetrics(0, 0, 0.0, 0.0, 0.0, 0.0, 0.0)

    q1 = _percentile(lap_times_sorted, 0.25)
    q3 = _percentile(lap_times_sorted, 0.75)

    return LapMetrics(
        lap_count=len(segments),
        complete_lap_count=len(lap_times_sorted),
        best_lap=min(lap_times_sorted),
        median_lap=median(lap_times_sorted),
        worst_lap=max(lap_times_sorted),
        stddev_lap=pstdev(lap_times_sorted) if len(lap_times_sorted) > 1 else 0.0,
        iqr_lap=q3 - q1,
    )


def override_best_lap(metrics: LapMetrics, best_lap_time: float) -> LapMetrics:
    if best_lap_time <= 0:
        return metrics
    if metrics.best_lap and metrics.best_lap > 0:
        best = min(metrics.best_lap, best_lap_time)
    else:
        best = best_lap_time
    return LapMetrics(
        lap_count=metrics.lap_count,
        complete_lap_count=metrics.complete_lap_count,
        best_lap=best,
        median_lap=metrics.median_lap,
        worst_lap=metrics.worst_lap,
        stddev_lap=metrics.stddev_lap,
        iqr_lap=metrics.iqr_lap,
    )


def incident_counts(player_incidents: Sequence[int], segments: Sequence[LapSegment]) -> Dict[int, int]:
    """Returns incident delta per lap number."""
    incidents_by_lap: Dict[int, int] = {}
    if not player_incidents:
        return incidents_by_lap

    last_inc = int(player_incidents[0])
    for seg in segments:
        end_inc = int(player_incidents[seg.end_idx])
        incidents_by_lap[seg.lap_number] = max(0, end_inc - last_inc)
        last_inc = end_inc

    return incidents_by_lap
