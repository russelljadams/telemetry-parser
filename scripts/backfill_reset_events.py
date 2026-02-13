#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telemetry_parser.ibt import IBTReader
from telemetry_parser.segments import detect_reset_events


FILENAME_RE = re.compile(
    r"(superformulalights324_(?P<track>.+?) (?P<date>\d{4}-\d{2}-\d{2}) (?P<time>\d{2}-\d{2}-\d{2})\.ibt)",
    re.IGNORECASE,
)


def parse_date(file_path: str) -> datetime | None:
    match = FILENAME_RE.search(Path(file_path).name)
    if not match:
        return None
    dt_str = f"{match.group('date')} {match.group('time').replace('-', ':')}"
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill reset events for existing sessions")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--start-date", help="Only process sessions on/after YYYY-MM-DD")
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None

    conn = sqlite3.connect(args.db)
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
    conn.commit()

    cur.execute("SELECT id, file_path FROM sessions")
    sessions: List[Tuple[int, str]] = cur.fetchall()

    cur.execute("SELECT DISTINCT session_id FROM reset_events")
    existing = {row[0] for row in cur.fetchall()}

    for session_id, file_path in sessions:
        if session_id in existing:
            continue
        dt = parse_date(file_path)
        if start_dt and (dt is None or dt < start_dt):
            continue
        if not FILENAME_RE.search(Path(file_path).name):
            continue
        if not Path(file_path).exists():
            continue

        reader = IBTReader(file_path).read()
        channels = {
            "Lap": reader.read_channel("Lap"),
            "LapDistPct": reader.read_channel("LapDistPct"),
            "SessionTime": reader.read_channel("SessionTime"),
        }
        reset_events = detect_reset_events(
            lap=channels["Lap"],
            lap_dist_pct=channels["LapDistPct"],
            session_time=channels["SessionTime"],
        )
        for event in reset_events:
            cur.execute(
                """
                INSERT INTO reset_events (
                    session_id, lap_number, lap_dist_pct, index_in_session
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    session_id,
                    event.lap_number,
                    event.lap_dist_pct,
                    event.index,
                ),
            )
        conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
