#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median, pstdev
from typing import Dict, Iterable, List, Tuple


FILENAME_RE = re.compile(
    r"(superformulalights324_(?P<track>.+?) (?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}-\d{2}-\d{2})\.ibt)",
    re.IGNORECASE,
)


def parse_file_metadata(file_path: str) -> Tuple[str, str, bool]:
    match = FILENAME_RE.search(Path(file_path).name)
    if not match:
        return "unknown", "unknown", False
    track = match.group("track").strip()
    date = match.group("date")
    return track, date, True


def build_bins(values: Iterable[float], bin_size: float = 0.05) -> List[Dict[str, float]]:
    counts: Dict[int, int] = defaultdict(int)
    for val in values:
        if val is None:
            continue
        idx = int(val // bin_size)
        counts[idx] += 1

    bins = []
    for idx in sorted(counts.keys()):
        start = idx * bin_size
        end = start + bin_size
        bins.append({"startPct": round(start, 3), "endPct": round(end, 3), "count": counts[idx]})
    return bins


def main() -> None:
    parser = argparse.ArgumentParser(description="Build JSON data for the website")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--output", default="../russelljadams/public/data", help="Output folder for JSON data")
    args = parser.parse_args()

    db_path = Path(args.db)
    output_root = Path(args.output).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reset_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            lap_number INTEGER,
            lap_dist_pct REAL,
            index_in_session INTEGER
        );
        """
    )

    cur.execute(
        """
        SELECT id, file_path, session_start_time, session_end_time, session_lap_count
        FROM sessions
        """
    )
    sessions = cur.fetchall()

    cur.execute(
        """
        SELECT session_id, lap_dist_pct
        FROM reset_events
        """
    )
    reset_rows = cur.fetchall()

    resets_by_session: Dict[int, List[float]] = defaultdict(list)
    for session_id, lap_dist_pct in reset_rows:
        resets_by_session[int(session_id)].append(float(lap_dist_pct))

    total_sessions = 0
    total_laps = 0
    total_duration_s = 0.0
    total_resets = 0
    jan_2026_sessions = 0
    jan_2026_duration_s = 0.0
    feb_2026_sessions = 0
    feb_2026_duration_s = 0.0

    resets_all = [val for values in resets_by_session.values() for val in values]
    overall_bins = build_bins(resets_all)

    daily_data: Dict[str, Dict[str, object]] = {}
    track_data: Dict[str, Dict[str, object]] = {}

    session_track_date: Dict[int, Tuple[str, str]] = {}
    for session_id, file_path, start_time, end_time, lap_count in sessions:
        track, date_str, is_sfl = parse_file_metadata(file_path)
        if not is_sfl:
            continue
        if date_str < "2026-01-01" or date_str > "2026-12-31":
            continue
        duration_s = (end_time or 0) - (start_time or 0)
        laps = lap_count or 0
        resets = resets_by_session.get(int(session_id), [])
        total_sessions += 1
        total_laps += laps
        total_duration_s += duration_s
        total_resets += len(resets)

        session_track_date[int(session_id)] = (track, date_str)

        if date_str.startswith("2026-01-"):
            jan_2026_sessions += 1
            jan_2026_duration_s += duration_s
        elif date_str.startswith("2026-02-"):
            feb_2026_sessions += 1
            feb_2026_duration_s += duration_s

        day = daily_data.setdefault(date_str, {
            "date": date_str,
            "sessions": 0,
            "laps": 0,
            "durationSeconds": 0.0,
            "resets": 0,
            "resetHotspotsBins": [],
            "tracks": defaultdict(lambda: {
                "sessions": 0,
                "laps": 0,
                "durationSeconds": 0.0,
                "resets": 0,
                "resetHotspotsBins": [],
            }),
        })

        day["sessions"] += 1
        day["laps"] += laps
        day["durationSeconds"] += duration_s
        day["resets"] += len(resets)

        track_entry = day["tracks"][track]
        track_entry["sessions"] += 1
        track_entry["laps"] += laps
        track_entry["durationSeconds"] += duration_s
        track_entry["resets"] += len(resets)

        track_root = track_data.setdefault(track, {
            "track": track,
            "sessions": 0,
            "laps": 0,
            "durationSeconds": 0.0,
            "resets": 0,
            "resetHotspotsBins": [],
        })
        track_root["sessions"] += 1
        track_root["laps"] += laps
        track_root["durationSeconds"] += duration_s
        track_root["resets"] += len(resets)

        track_root.setdefault("_reset_values", []).extend(resets)
        day.setdefault("_reset_values", []).extend(resets)
        track_entry.setdefault("_reset_values", []).extend(resets)

    for date_str, day in daily_data.items():
        day["resetHotspotsBins"] = build_bins(day.get("_reset_values", []))
        for track_name, track_entry in list(day["tracks"].items()):
            track_entry["resetHotspotsBins"] = build_bins(track_entry.get("_reset_values", []))
            track_entry.pop("_reset_values", None)
            day["tracks"][track_name] = track_entry
        day.pop("_reset_values", None)
        day["tracks"] = dict(day["tracks"])

    for track_name, track_entry in track_data.items():
        track_entry["resetHotspotsBins"] = build_bins(track_entry.get("_reset_values", []))
        track_entry.pop("_reset_values", None)
        track_data[track_name] = track_entry

    # Track daily time-series (best/median/stddev on complete laps)
    cur.execute(
        """
        SELECT session_id, lap_time
        FROM laps
        WHERE is_complete = 1 AND lap_time > 0
        """
    )
    lap_rows = cur.fetchall()
    lap_times_by_track_day: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for session_id, lap_time in lap_rows:
        key = session_track_date.get(int(session_id))
        if not key:
            continue
        track, date_str = key
        lap_times_by_track_day[(track, date_str)].append(float(lap_time))

    track_timeseries: Dict[str, List[Dict[str, float]]] = defaultdict(list)
    for (track, date_str), times in sorted(lap_times_by_track_day.items()):
        if not times:
            continue
        best = min(times)
        med = median(times)
        stddev = pstdev(times) if len(times) > 1 else 0.0
        track_timeseries[track].append(
            {
                "date": date_str,
                "bestLap": round(best, 3),
                "medianLap": round(med, 3),
                "stdDev": round(stddev, 3),
                "completeLaps": len(times),
            }
        )

    latest_day = max(daily_data.keys()) if daily_data else None

    summary = {
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "totalSessions": total_sessions,
        "totalLaps": total_laps,
        "totalHours": round(total_duration_s / 3600, 2),
        "totalResets": total_resets,
        "resetHotspotsBins": overall_bins,
        "latestDay": latest_day,
        "jan2026Sessions": jan_2026_sessions,
        "jan2026Hours": round(jan_2026_duration_s / 3600, 2),
        "feb2026Sessions": feb_2026_sessions,
        "feb2026Hours": round(feb_2026_duration_s / 3600, 2),
    }

    (output_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    daily_dir = output_root / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    for date_str, day in daily_data.items():
        day_out = dict(day)
        day_out["durationHours"] = round(day_out.pop("durationSeconds") / 3600, 2)
        for track_name, track_entry in day_out["tracks"].items():
            track_entry["durationHours"] = round(track_entry.pop("durationSeconds") / 3600, 2)
            day_out["tracks"][track_name] = track_entry
        (daily_dir / f"{date_str}.json").write_text(json.dumps(day_out, indent=2), encoding="utf-8")

    tracks_dir = output_root / "tracks"
    tracks_dir.mkdir(parents=True, exist_ok=True)
    for track_name, track_entry in track_data.items():
        track_entry["durationHours"] = round(track_entry.pop("durationSeconds") / 3600, 2)
        safe_name = track_name.replace(" ", "-")
        (tracks_dir / f"{safe_name}.json").write_text(json.dumps(track_entry, indent=2), encoding="utf-8")
        series = track_timeseries.get(track_name)
        if series:
            (tracks_dir / f"{safe_name}-timeseries.json").write_text(
                json.dumps(series, indent=2), encoding="utf-8"
            )

    conn.close()


if __name__ == "__main__":
    main()
