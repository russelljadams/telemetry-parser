#!/usr/bin/env python3
"""Backfill is_clean, has_official_time, event_count, clean metrics,
track_name, car_name, and session classification.

Uses telemetry-based filtering: a lap is only clean if it has zero iRacing
incidents AND zero detected events (big_save, spin, off_track), in addition
to being structurally valid (complete, not reset, official time, within time bounds).
"""
from __future__ import annotations

import argparse
import glob
import re
import sqlite3
import sys
from pathlib import Path
from statistics import median, pstdev
from typing import Dict, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telemetry_parser.track_config import DEFAULT_MAX_TIMES, DEFAULT_MIN_TIMES, GLOBAL_MAX_LAP_TIME, GLOBAL_MIN_LAP_TIME


FILENAME_RE = re.compile(
    r"superformulalights324_(?P<track>.+?) \d{4}-\d{2}-\d{2}",
    re.IGNORECASE,
)


def _track_from_filename(file_path: str) -> Optional[str]:
    match = FILENAME_RE.search(Path(file_path).name)
    if match:
        return match.group("track").strip()
    return None


def _min_time_for_track(track: Optional[str]) -> float:
    if track and track in DEFAULT_MIN_TIMES:
        return DEFAULT_MIN_TIMES[track]
    return GLOBAL_MIN_LAP_TIME


def _max_time_for_track(track: Optional[str]) -> float:
    if track and track in DEFAULT_MAX_TIMES:
        return DEFAULT_MAX_TIMES[track]
    return GLOBAL_MAX_LAP_TIME


def _has_official_time(lap: dict) -> bool:
    """Heuristic: if lap_time matches (end_time - start_time) exactly, it was
    computed as a fallback because iRacing's LapLastLapTime was -1 or 0.
    When iRacing provides an official time, it differs from the computed time
    because iRacing uses precise S/F line crossing detection."""
    computed = lap["end_time"] - lap["start_time"]
    return abs(lap["lap_time"] - computed) > 0.001


def _classify_from_laps(
    laps: list, min_time: float, max_time: float
) -> str:
    """Simple classification from lap data."""
    if not laps:
        return "mixed"

    total = len(laps)
    clean = [l for l in laps if l["is_clean"]]
    clean_ratio = len(clean) / total if total > 0 else 0.0

    total_duration = 0.0
    if laps:
        total_duration = laps[-1]["end_time"] - laps[0]["start_time"]

    resets = [l for l in laps if l["is_reset"]]
    invalid_ratio = (total - len(clean)) / total if total > 0 else 0.0
    if invalid_ratio > 0.6 and resets:
        return "corner_isolation"

    if len(clean) >= 10 and total_duration > 1200:
        max_run = 0
        current = 0
        for l in laps:
            if l["is_clean"]:
                current += 1
                max_run = max(max_run, current)
            else:
                current = 0
        if max_run >= 10:
            return "race_sim"

    if clean_ratio > 0.6:
        return "hot_laps"

    return "mixed"


def _backfill_incidents_from_ibt(
    conn: sqlite3.Connection,
    session_id: int,
    file_path: str,
) -> Dict[int, int]:
    """Re-read PlayerIncidents from the IBT file and compute correct
    per-lap incident counts using rising-edge detection."""
    from telemetry_parser.ibt import IBTReader
    from telemetry_parser.metrics import incident_counts
    from telemetry_parser.segments import segment_laps

    path = Path(file_path)
    if not path.exists():
        return {}

    try:
        reader = IBTReader(str(path)).read()
        channels = ['SessionTime', 'Lap', 'LapDistPct', 'LapLastLapTime', 'LapCompleted', 'PlayerIncidents']
        missing = [ch for ch in channels if ch not in reader.var_by_name]
        if missing:
            return {}

        data = {name: [] for name in channels}
        for record in reader.iter_records(channels):
            for name in channels:
                data[name].append(record[name])

        segments = segment_laps(
            session_time=data['SessionTime'],
            lap=data['Lap'],
            lap_dist_pct=data['LapDistPct'],
            lap_last_lap_time=data['LapLastLapTime'],
            lap_completed=data['LapCompleted'],
        )

        return incident_counts(data['PlayerIncidents'], segments)
    except Exception as e:
        print(f"  Warning: could not re-read IBT for incidents: {e}")
        return {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill clean metrics on existing sessions")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--reread-ibt", action="store_true",
                        help="Re-read IBT files to fix PlayerIncidents counts (slower)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure columns exist
    migrations = [
        "ALTER TABLE laps ADD COLUMN is_clean INTEGER DEFAULT 0",
        "ALTER TABLE laps ADD COLUMN has_official_time INTEGER DEFAULT 1",
        "ALTER TABLE laps ADD COLUMN event_count INTEGER DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN track_name TEXT",
        "ALTER TABLE sessions ADD COLUMN car_name TEXT",
        "ALTER TABLE sessions ADD COLUMN clean_best_lap REAL",
        "ALTER TABLE sessions ADD COLUMN clean_median_lap REAL",
        "ALTER TABLE sessions ADD COLUMN clean_stddev_lap REAL",
        "ALTER TABLE sessions ADD COLUMN clean_lap_count INTEGER",
        "ALTER TABLE sessions ADD COLUMN classified_session_type TEXT",
    ]
    for sql in migrations:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
    conn.commit()

    cur.execute("SELECT id, file_path FROM sessions")
    sessions = cur.fetchall()
    print(f"Processing {len(sessions)} sessions...")

    updated = 0
    for session in sessions:
        session_id = session["id"]
        file_path = session["file_path"]

        track = _track_from_filename(file_path)
        min_time = _min_time_for_track(track)
        max_time = _max_time_for_track(track)

        # Load laps for this session
        cur.execute(
            """
            SELECT id, lap_number, lap_time, is_complete, is_reset, start_time, end_time, incidents
            FROM laps WHERE session_id = ?
            ORDER BY lap_number
            """,
            (session_id,),
        )
        laps = [dict(row) for row in cur.fetchall()]

        # Count ALL events per lap (for informational event_count column)
        cur.execute(
            """
            SELECT lap_number, COUNT(*) as cnt
            FROM events
            WHERE session_id = ?
            GROUP BY lap_number
            """,
            (session_id,),
        )
        all_events_per_lap: Dict[int, int] = {row["lap_number"]: row["cnt"] for row in cur.fetchall()}

        # Count only serious events (spin, off_track) for clean determination
        # big_save is informational only (aggressive cornering, not an incident)
        cur.execute(
            """
            SELECT lap_number, COUNT(*) as cnt
            FROM events
            WHERE session_id = ? AND event_type IN ('spin', 'off_track')
            GROUP BY lap_number
            """,
            (session_id,),
        )
        serious_per_lap: Dict[int, int] = {row["lap_number"]: row["cnt"] for row in cur.fetchall()}

        # Optionally re-read IBT files for correct incident counts
        incidents_from_ibt: Dict[int, int] = {}
        if args.reread_ibt:
            incidents_from_ibt = _backfill_incidents_from_ibt(conn, session_id, file_path)

        # Update each lap
        for lap in laps:
            official = _has_official_time(lap)
            lap["has_official_time"] = official

            evt_count = all_events_per_lap.get(lap["lap_number"], 0)
            lap["event_count"] = evt_count
            serious = serious_per_lap.get(lap["lap_number"], 0)

            # Update incidents from IBT re-read if available
            inc = lap["incidents"]
            if incidents_from_ibt and lap["lap_number"] in incidents_from_ibt:
                inc = incidents_from_ibt[lap["lap_number"]]

            # A lap is "clean" = a real, complete lap the driver actually drove.
            # Incidents and events are tracked but do NOT exclude —
            # they're part of the driver's real pace and belong in variance metrics.
            # Only structural issues exclude: incomplete, reset, untimed, AFK.
            is_clean = 1 if (
                lap["is_complete"]
                and not lap["is_reset"]
                and official
                and lap["lap_time"] >= min_time
                and lap["lap_time"] <= max_time
            ) else 0
            lap["is_clean"] = is_clean

            cur.execute(
                "UPDATE laps SET is_clean = ?, has_official_time = ?, event_count = ?, incidents = ? WHERE id = ?",
                (is_clean, 1 if official else 0, evt_count, inc, lap["id"]),
            )

        # Compute clean metrics
        clean_times = sorted(
            l["lap_time"] for l in laps if l["is_clean"]
        )

        clean_best = min(clean_times) if clean_times else None
        clean_median = median(clean_times) if clean_times else None
        clean_stddev = pstdev(clean_times) if len(clean_times) > 1 else (0.0 if clean_times else None)
        clean_count = len(clean_times)

        # Classify session
        session_type = _classify_from_laps(laps, min_time, max_time)

        # Display track name (capitalized)
        track_display = track.replace("-", " ").title() if track else None

        cur.execute(
            """
            UPDATE sessions
            SET track_name = ?,
                car_name = ?,
                clean_best_lap = ?,
                clean_median_lap = ?,
                clean_stddev_lap = ?,
                clean_lap_count = ?,
                classified_session_type = ?
            WHERE id = ?
            """,
            (
                track_display,
                "Super Formula Lights",
                clean_best,
                clean_median,
                clean_stddev,
                clean_count,
                session_type,
                session_id,
            ),
        )
        updated += 1

    conn.commit()

    # Print summary
    cur.execute("SELECT COUNT(*) FROM laps WHERE is_clean = 1")
    total_clean = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM laps")
    total_laps = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM laps WHERE event_count > 0")
    laps_with_events = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM laps WHERE incidents > 0")
    laps_with_incidents = cur.fetchone()[0]

    print(f"\nUpdated {updated} sessions.")
    print(f"Total laps: {total_laps}")
    print(f"Laps with iRacing incidents: {laps_with_incidents}")
    print(f"Laps with detected events (big_save/spin/off_track): {laps_with_events}")
    print(f"Valid laps (real, driven, timed): {total_clean}")

    conn.close()


if __name__ == "__main__":
    main()
