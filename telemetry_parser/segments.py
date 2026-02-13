from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence


RESET_DROP_THRESHOLD = 0.05
# If a "completed" lap is shorter than this, it's almost certainly a reset.
# iRacing increments the lap counter on reset, so we can't rely on
# lap_increment alone. The shortest real lap on any track is ~90s.
RESET_MAX_DURATION = 60.0


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
    has_official_time: bool = True


@dataclass(frozen=True)
class ResetEvent:
    lap_number: int
    lap_dist_pct: float
    index: int


def segment_laps(
    session_time: Sequence[float],
    lap: Sequence[int],
    lap_dist_pct: Sequence[float],
    lap_last_lap_time: Sequence[float],
    lap_completed: Sequence[int],
) -> List[LapSegment]:
    if not (len(session_time) == len(lap) == len(lap_dist_pct) == len(lap_last_lap_time) == len(lap_completed)):
        raise ValueError("All input channels must be the same length")
    if not session_time:
        return []

    segments: List[LapSegment] = []
    start_idx = 0
    start_lap = int(lap[0])
    last_lap_completed = int(lap_completed[0])

    for i in range(1, len(session_time)):
        lap_dist_drop = lap_dist_pct[i] < lap_dist_pct[i - 1] - RESET_DROP_THRESHOLD
        lap_increment = lap[i] > lap[i - 1]

        if lap_dist_drop or lap_increment:
            end_idx = i - 1
            end_time = float(session_time[end_idx])
            raw_lap_time = float(lap_last_lap_time[i])

            completed_now = int(lap_completed[i])
            duration = end_time - float(session_time[start_idx])
            is_complete = completed_now > last_lap_completed or lap_increment
            # A reset is either: (1) LapDistPct drops without lap increment
            # (classic detection), or (2) a "completed" lap that's way too
            # short to be real — iRacing increments the lap counter on
            # reset, making these look like normal laps.
            is_reset = (lap_dist_drop and not lap_increment) or (
                lap_increment and duration < RESET_MAX_DURATION
            )

            has_official_time = raw_lap_time > 0.0
            if raw_lap_time <= 0.0:
                raw_lap_time = float(session_time[end_idx] - session_time[start_idx])

            segments.append(LapSegment(
                lap_number=int(lap[i - 1]),
                start_idx=start_idx,
                end_idx=end_idx,
                start_time=float(session_time[start_idx]),
                end_time=end_time,
                lap_time=raw_lap_time,
                is_complete=is_complete,
                is_reset=is_reset,
                has_official_time=has_official_time,
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


def detect_reset_events(
    lap: Sequence[int],
    lap_dist_pct: Sequence[float],
    session_time: Sequence[float] | None = None,
) -> List[ResetEvent]:
    if not (len(lap) == len(lap_dist_pct)):
        raise ValueError("lap and lap_dist_pct must be the same length")

    resets: List[ResetEvent] = []
    last_boundary_idx = 0
    for i in range(1, len(lap)):
        lap_dist_drop = lap_dist_pct[i] < lap_dist_pct[i - 1] - RESET_DROP_THRESHOLD
        lap_increment = lap[i] > lap[i - 1]

        if lap_dist_drop or lap_increment:
            classic_reset = lap_dist_drop and not lap_increment
            # Check for short-duration "lap" that's really a reset
            duration_reset = False
            if lap_increment and session_time is not None:
                duration = float(session_time[i - 1]) - float(session_time[last_boundary_idx])
                duration_reset = duration < RESET_MAX_DURATION

            if classic_reset or duration_reset:
                resets.append(ResetEvent(
                    lap_number=int(lap[i - 1]),
                    lap_dist_pct=float(lap_dist_pct[i - 1]),
                    index=i - 1,
                ))
            last_boundary_idx = i

    return resets
