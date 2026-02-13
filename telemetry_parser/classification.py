from __future__ import annotations

from typing import Sequence

from .segments import LapSegment, ResetEvent


def classify_session(
    segments: Sequence[LapSegment],
    reset_events: Sequence[ResetEvent],
    min_valid_lap_time: float = 0.0,
) -> str:
    """Classify a session based on lap and reset patterns.

    Returns one of: corner_isolation, hot_laps, race_sim, mixed.
    """
    if not segments:
        return "mixed"

    clean_laps = [
        seg for seg in segments
        if seg.is_complete and not seg.is_reset and seg.lap_time >= min_valid_lap_time
    ]
    invalid_or_reset = [
        seg for seg in segments
        if seg.is_reset or not seg.is_complete or seg.lap_time < min_valid_lap_time
    ]

    total = len(segments)
    clean_count = len(clean_laps)
    invalid_count = len(invalid_or_reset)

    clean_ratio = clean_count / total if total > 0 else 0.0
    invalid_ratio = invalid_count / total if total > 0 else 0.0

    # Corner isolation: mostly resets/invalid, resets clustered in narrow band
    if invalid_ratio > 0.6 and reset_events:
        pcts = [e.lap_dist_pct for e in reset_events]
        pct_range = max(pcts) - min(pcts) if len(pcts) > 1 else 0.0
        if pct_range < 0.3:
            return "corner_isolation"

    # Race sim: 10+ consecutive clean laps, session >20 minutes
    if clean_count >= 10:
        session_duration = 0.0
        if segments:
            session_duration = segments[-1].end_time - segments[0].start_time
        if session_duration > 1200:  # 20 minutes
            max_consecutive = _max_consecutive_clean(segments, min_valid_lap_time)
            if max_consecutive >= 10:
                return "race_sim"

    # Hot laps: mostly clean
    if clean_ratio > 0.6:
        return "hot_laps"

    return "mixed"


def _max_consecutive_clean(
    segments: Sequence[LapSegment],
    min_valid_lap_time: float,
) -> int:
    max_run = 0
    current_run = 0
    for seg in segments:
        if seg.is_complete and not seg.is_reset and seg.lap_time >= min_valid_lap_time:
            current_run += 1
            max_run = max(max_run, current_run)
        else:
            current_run = 0
    return max_run
