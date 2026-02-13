#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telemetry_parser.ingest import ingest_file


# Porsche 911 GT3 Cup — iRacing car ID is "porsche9922cup"
# Filename format: porsche9922cup_<track> YYYY-MM-DD HH-MM-SS.ibt
FILENAME_RE = re.compile(
    r"(porsche9922cup_(?P<track>.+?) (?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}-\d{2}-\d{2})\.ibt)",
    re.IGNORECASE,
)

# Legacy SFL pattern — kept for archived data compatibility
SFL_FILENAME_RE = re.compile(
    r"(superformulalights324_(?P<track>.+?) (?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}-\d{2}-\d{2})\.ibt)",
    re.IGNORECASE,
)

BASELINE_RE = re.compile(
    r"BASELINE_(?P<track>[A-Za-z]+)_(?P<month>\d{1,2})-(?P<day>\d{1,2})-(?P<year>\d{2,4})_",
    re.IGNORECASE,
)

BASELINE_TRACK_MAP = {
    "SPA": "spa",
    "MONZA": "monza full",
    "NURBURGRING": "nurburgring gp",
    "BARCELONA": "barcelona gp",
}

# iRacing track names from filenames → normalized experiment track IDs.
# Add entries as new tracks are driven and filenames are confirmed.
TRACK_NAME_MAP = {
    "spa 2024 up": "spa",
    "monza full": "monza full",
    # Confirm these once first IBT files are generated:
    # "nurburgring grand prix": "nurburgring gp",
    # "circuit de barcelona": "barcelona gp",
}


@dataclass
class ParsedFile:
    path: str
    track: str
    dt: datetime


def normalize_track(raw_track: str) -> str:
    """Map iRacing track names to experiment track IDs."""
    lower = raw_track.lower().strip()
    return TRACK_NAME_MAP.get(lower, raw_track)


def parse_filename(path: Path) -> Optional[ParsedFile]:
    # Try Porsche 911 GT3 Cup pattern first
    match = FILENAME_RE.search(path.name)
    if not match:
        # Legacy SFL pattern (archived data)
        match = SFL_FILENAME_RE.search(path.name)
    if match:
        track = normalize_track(match.group("track").strip())
        dt_str = f"{match.group('date')} {match.group('time').replace('-', ':')}"
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        return ParsedFile(path=str(path), track=track, dt=dt)

    # Fallback: baseline filename pattern (BASELINE_TRACK_M-D-YY_...)
    bmatch = BASELINE_RE.search(path.name)
    if bmatch:
        track_key = bmatch.group("track").upper()
        track = BASELINE_TRACK_MAP.get(track_key)
        if not track:
            return None
        month = int(bmatch.group("month"))
        day = int(bmatch.group("day"))
        year = int(bmatch.group("year"))
        if year < 100:
            year += 2000
        try:
            dt = datetime(year, month, day)
        except ValueError:
            return None
        return ParsedFile(path=str(path), track=track, dt=dt)

    return None


def load_existing_sessions(conn: sqlite3.Connection) -> Dict[str, int]:
    cur = conn.cursor()
    cur.execute("SELECT id, file_path FROM sessions")
    return {row[1]: row[0] for row in cur.fetchall()}


def summarize_sessions(
    conn: sqlite3.Connection, file_paths: Iterable[str]
) -> Tuple[int, int, float]:
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in file_paths)
    if not placeholders:
        return 0, 0, 0.0
    cur.execute(
        f"""
        SELECT
            COUNT(*),
            COALESCE(SUM(session_lap_count), 0),
            COALESCE(SUM(session_end_time - session_start_time), 0)
        FROM sessions
        WHERE file_path IN ({placeholders})
        """,
        list(file_paths),
    )
    count, laps, duration = cur.fetchone()
    return int(count), int(laps), float(duration)


def write_daily_report(
    output_path: Path,
    day: date,
    rows: List[Tuple[str, int, str, str]],
    totals: Tuple[int, int, float],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    session_count, total_laps, total_duration_s = totals

    lines: List[str] = []
    lines.append(f"# Daily Ingest Report — {day.isoformat()}")
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"Sessions (files): {session_count}")
    lines.append(f"Total laps (session_lap_count sum): {total_laps}")
    lines.append(f"Total time (hours): {total_duration_s / 3600:.2f}")
    lines.append("")
    lines.append("## Sessions")
    lines.append("Session ID | Track | Timestamp | File")
    lines.append("--- | --- | --- | ---")
    for file_path, session_id, track, timestamp in rows:
        lines.append(f"{session_id} | {track} | {timestamp} | {file_path}")

    output_path.write_text("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily ingest for Porsche 911 GT3 Cup .ibt files")
    parser.add_argument("--source", default="/media/sf_iracing", help="Root folder to scan for .ibt files")
    parser.add_argument("--start-date", required=True, help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Exclusive end date (YYYY-MM-DD)")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--reports", default="reports", help="Reports output directory")
    parser.add_argument("--summaries", default="summaries", help="Publishable summaries output directory")
    parser.add_argument("--daily-reports", default="reports/daily", help="Daily report output directory")
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    source = Path(args.source)
    db_path = Path(args.db)
    report_dir = Path(args.reports)
    summary_dir = Path(args.summaries)
    daily_report_dir = Path(args.daily_reports)

    conn = sqlite3.connect(db_path)
    existing = load_existing_sessions(conn)

    parsed_files: List[ParsedFile] = []
    for path in source.rglob("*.ibt"):
        if "/2025/" in str(path):
            continue
        parsed = parse_filename(path)
        if not parsed:
            continue
        if parsed.dt < start_dt:
            continue
        if end_dt and parsed.dt >= end_dt:
            continue
        parsed_files.append(parsed)

    parsed_files.sort(key=lambda p: p.dt)

    by_day: Dict[date, List[ParsedFile]] = defaultdict(list)
    for parsed in parsed_files:
        by_day[parsed.dt.date()].append(parsed)

    for day in sorted(by_day.keys()):
        rows: List[Tuple[str, int, str, str]] = []
        for parsed in by_day[day]:
            file_path = parsed.path
            track = parsed.track
            timestamp = parsed.dt.strftime("%Y-%m-%d %H:%M:%S")

            session_id = existing.get(file_path)
            if session_id is None:
                session_id = ingest_file(file_path, str(db_path), str(report_dir), str(summary_dir))
                existing[file_path] = session_id

            # Auto-flag sessions from baselines/ subfolder
            if "/baselines/" in file_path.lower() or "\\baselines\\" in file_path.lower():
                conn.execute("UPDATE sessions SET is_baseline = 1 WHERE id = ? AND is_baseline = 0", (session_id,))
                conn.commit()

            rows.append((file_path, session_id, track, timestamp))

        totals = summarize_sessions(conn, [r[0] for r in rows])
        daily_report_path = daily_report_dir / f"{day.isoformat()}.md"
        write_daily_report(daily_report_path, day, rows, totals)

    conn.close()


if __name__ == "__main__":
    main()
