from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class IncidentEvent:
    event_type: str
    index: int
    session_time: float
    lap_number: int


def _sample_rate(session_time: Sequence[float]) -> float:
    if len(session_time) < 2:
        return 0.0
    deltas = [session_time[i] - session_time[i - 1] for i in range(1, len(session_time))]
    deltas = [d for d in deltas if d > 0]
    if not deltas:
        return 0.0
    return 1.0 / median(deltas)


def detect_events(
    session_time: Sequence[float],
    lap: Sequence[int],
    speed: Sequence[float],
    yaw_rate: Sequence[float],
    steering_angle: Sequence[float],
    is_on_track: Sequence[int],
) -> List[IncidentEvent]:
    """Detect off-track, spin, and big-save events using heuristic thresholds.

    Heuristics are intentionally conservative to avoid false positives.
    """
    if not (len(session_time) == len(lap) == len(speed) == len(yaw_rate) == len(steering_angle) == len(is_on_track)):
        raise ValueError("All input channels must be the same length")

    hz = _sample_rate(session_time)
    if hz <= 0:
        hz = 60.0

    min_off_track_samples = int(max(1, 0.5 * hz))
    min_spin_samples = int(max(1, 0.5 * hz))
    cooldown_samples = int(max(1, 1.5 * hz))

    spin_yaw_rate = 2.0  # rad/s
    save_yaw_rate = 1.2  # rad/s
    min_spin_speed = 8.0  # m/s
    min_save_speed = 12.0  # m/s
    min_save_steer = 0.4  # rad

    events: List[IncidentEvent] = []
    cooldown = 0
    off_track_run = 0
    spin_run = 0

    for i in range(len(session_time)):
        if cooldown > 0:
            cooldown -= 1

        on_track = bool(is_on_track[i])
        if not on_track:
            off_track_run += 1
        else:
            if off_track_run >= min_off_track_samples:
                events.append(IncidentEvent(
                    event_type="off_track",
                    index=i - 1,
                    session_time=float(session_time[i - 1]),
                    lap_number=int(lap[i - 1]),
                ))
                cooldown = cooldown_samples
            off_track_run = 0

        if cooldown > 0:
            spin_run = 0
            continue

        if abs(yaw_rate[i]) >= spin_yaw_rate and speed[i] >= min_spin_speed:
            spin_run += 1
        else:
            if spin_run >= min_spin_samples:
                events.append(IncidentEvent(
                    event_type="spin",
                    index=i - 1,
                    session_time=float(session_time[i - 1]),
                    lap_number=int(lap[i - 1]),
                ))
                cooldown = cooldown_samples
            spin_run = 0

        if cooldown == 0:
            if abs(yaw_rate[i]) >= save_yaw_rate and speed[i] >= min_save_speed and abs(steering_angle[i]) >= min_save_steer:
                events.append(IncidentEvent(
                    event_type="big_save",
                    index=i,
                    session_time=float(session_time[i]),
                    lap_number=int(lap[i]),
                ))
                cooldown = cooldown_samples

    return events


def summarize_events(events: Sequence[IncidentEvent]) -> Dict[str, int]:
    summary: Dict[str, int] = {}
    for event in events:
        summary[event.event_type] = summary.get(event.event_type, 0) + 1
    return summary


def event_counts_by_lap(events: Sequence[IncidentEvent]) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    for event in events:
        counts[event.lap_number] = counts.get(event.lap_number, 0) + 1
    return counts


# big_save is informational (aggressive cornering), not an actual incident.
# Only spin and off_track disqualify a lap from being clean.
SERIOUS_EVENT_TYPES = frozenset({"spin", "off_track"})


def serious_event_counts_by_lap(events: Sequence[IncidentEvent]) -> Dict[int, int]:
    """Count only spin and off_track events per lap (excludes big_save)."""
    counts: Dict[int, int] = {}
    for event in events:
        if event.event_type in SERIOUS_EVENT_TYPES:
            counts[event.lap_number] = counts.get(event.lap_number, 0) + 1
    return counts


def hotspot_buckets(
    events: Sequence[IncidentEvent],
    lap_dist_pct: Sequence[float],
    bucket_size: float = 0.05,
) -> List[Tuple[float, float, int]]:
    buckets: Dict[float, int] = {}
    for event in events:
        if event.index < 0 or event.index >= len(lap_dist_pct):
            continue
        pct = float(lap_dist_pct[event.index])
        if pct < 0:
            continue
        bucket_start = (pct // bucket_size) * bucket_size
        bucket_start = round(bucket_start, 4)
        buckets[bucket_start] = buckets.get(bucket_start, 0) + 1

    results = []
    for bucket_start, count in buckets.items():
        results.append((bucket_start, round(bucket_start + bucket_size, 4), count))
    results.sort(key=lambda item: item[2], reverse=True)
    return results
