from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence


@dataclass(frozen=True)
class LapSegment:
    lap_number: int
    start_idx: int
    end_idx: int
    start_time: float
    end_time: float
    lap_time: float
    is_complete: bool
    is_reset: bool


def segment_laps(
    session_time: Sequence[float],
    lap: Sequence[int],
    lap_dist_pct: Sequence[float],
    lap_last_lap_time: Sequence[float],
    lap_completed: Sequence[int],
) -> List[LapSegment]:
    if not (len(session_time) == len(lap) == len(lap_dist_pct) == len(lap_last_lap_time) == len(lap_completed)):
        raise ValueError("All input channels must be the same length")

    segments: List[LapSegment] = []
    start_idx = 0
    start_lap = int(lap[0])
    last_lap_completed = int(lap_completed[0])

    for i in range(1, len(session_time)):
        lap_dist_drop = lap_dist_pct[i] < lap_dist_pct[i - 1] - 0.5
        lap_increment = lap[i] > lap[i - 1]

        if lap_dist_drop or lap_increment:
            end_idx = i - 1
            end_time = float(session_time[end_idx])
            last_lap_time = float(lap_last_lap_time[i])

            completed_now = int(lap_completed[i])
            is_complete = completed_now > last_lap_completed or lap_increment
            is_reset = lap_dist_drop and not lap_increment

            if last_lap_time <= 0.0:
                last_lap_time = float(session_time[end_idx] - session_time[start_idx])

            segments.append(LapSegment(
                lap_number=int(lap[i - 1]),
                start_idx=start_idx,
                end_idx=end_idx,
                start_time=float(session_time[start_idx]),
                end_time=end_time,
                lap_time=last_lap_time,
                is_complete=is_complete,
                is_reset=is_reset,
            ))

            start_idx = i
            start_lap = int(lap[i])
            last_lap_completed = completed_now

    if start_idx < len(session_time) - 1:
        end_idx = len(session_time) - 1
        segments.append(LapSegment(
            lap_number=int(lap[end_idx]),
            start_idx=start_idx,
            end_idx=end_idx,
            start_time=float(session_time[start_idx]),
            end_time=float(session_time[end_idx]),
            lap_time=float(session_time[end_idx] - session_time[start_idx]),
            is_complete=False,
            is_reset=False,
        ))

    return segments
