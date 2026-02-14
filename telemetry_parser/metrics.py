from __future__ import annotations

from dataclasses import dataclass
from statistics import median, pstdev
from typing import Dict, Iterable, List, Optional, Sequence

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


@dataclass(frozen=True)
class CleanMetrics:
    clean_lap_count: int
    clean_best_lap: float
    clean_median_lap: float
    clean_stddev_lap: float


def _percentile(sorted_vals: Sequence[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def is_valid_lap(seg: LapSegment, min_time: float = 0.0, max_time: float = 0.0) -> bool:
    if not seg.is_complete or seg.is_reset:
        return False
    if not seg.has_official_time:
        return False
    if seg.lap_time < min_time:
        return False
    if max_time > 0 and seg.lap_time > max_time:
        return False
    return True


def compute_lap_metrics(
    segments: Sequence[LapSegment],
    min_valid_lap_time: float = 0.0,
    max_valid_lap_time: float = 0.0,
) -> LapMetrics:
    lap_times = [
        seg.lap_time
        for seg in segments
        if is_valid_lap(seg, min_valid_lap_time, max_valid_lap_time)
    ]
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


def identify_outlaps(
    segments: Sequence[LapSegment],
    min_valid_lap_time: float = 0.0,
    max_valid_lap_time: float = 0.0,
) -> set:
    """Return lap numbers of outlaps (first valid lap on cold tyres).

    After a reset, the driver often does an untimed recovery lap back to the
    S/F line. That recovery lap warms the tyres, so the next valid lap is NOT
    an outlap. Only flag the first valid lap as outlap when no full-length
    warmup lap preceded it.
    """
    from .segments import RESET_MAX_DURATION

    outlap_laps: set = set()
    need_outlap = True  # first valid lap is always an outlap
    for seg in segments:
        if seg.is_reset:
            need_outlap = True
        elif seg.is_complete and seg.lap_time > RESET_MAX_DURATION:
            # A full-length driven lap — tyres are warm after this
            if need_outlap and is_valid_lap(seg, min_valid_lap_time, max_valid_lap_time):
                # First valid lap and tyres were cold → outlap
                outlap_laps.add(seg.lap_number)
            need_outlap = False
    return outlap_laps


def is_clean_lap(
    seg: LapSegment,
    min_time: float = 0.0,
    max_time: float = 0.0,
) -> bool:
    """A clean lap is any valid lap within the time bounds."""
    return is_valid_lap(seg, min_time, max_time)


def compute_clean_metrics(
    segments: Sequence[LapSegment],
    min_valid_lap_time: float = 0.0,
    max_valid_lap_time: float = 0.0,
    incidents_by_lap: Optional[Dict[int, int]] = None,
    events_by_lap: Optional[Dict[int, int]] = None,
) -> CleanMetrics:
    """Compute metrics for clean laps: any valid lap within the time bounds."""
    clean_times = []
    for seg in segments:
        if not is_clean_lap(seg, min_valid_lap_time, max_valid_lap_time):
            continue
        clean_times.append(seg.lap_time)
    clean_times.sort()
    if not clean_times:
        return CleanMetrics(0, 0.0, 0.0, 0.0)
    return CleanMetrics(
        clean_lap_count=len(clean_times),
        clean_best_lap=min(clean_times),
        clean_median_lap=median(clean_times),
        clean_stddev_lap=pstdev(clean_times) if len(clean_times) > 1 else 0.0,
    )


def override_best_lap(metrics: LapMetrics, best_lap_time: float) -> LapMetrics:
    if best_lap_time <= 0:
        return metrics
    return LapMetrics(
        lap_count=metrics.lap_count,
        complete_lap_count=metrics.complete_lap_count,
        best_lap=best_lap_time,
        median_lap=metrics.median_lap,
        worst_lap=metrics.worst_lap,
        stddev_lap=metrics.stddev_lap,
        iqr_lap=metrics.iqr_lap,
    )


def incident_counts(player_incidents: Sequence[int], segments: Sequence[LapSegment]) -> Dict[int, int]:
    """Returns incident count per lap number.

    PlayerIncidents is a pulse/flag channel: it spikes to 1 or 2 for a single
    tick then immediately returns to 0.  We count rising edges (0→N transitions)
    within each segment's index range and sum the incident values.
    """
    incidents_by_lap: Dict[int, int] = {}
    if not player_incidents:
        return incidents_by_lap

    for seg in segments:
        total = 0
        for i in range(seg.start_idx, min(seg.end_idx + 1, len(player_incidents))):
            val = int(player_incidents[i])
            prev = int(player_incidents[i - 1]) if i > 0 else 0
            if val > 0 and prev == 0:
                total += val
        incidents_by_lap[seg.lap_number] = total

    return incidents_by_lap
