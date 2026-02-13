#!/usr/bin/env python3
"""Flag or unflag sessions as baseline runs."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _ensure_column(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(sessions)")
    if not any(row[1] == "is_baseline" for row in cur.fetchall()):
        cur.execute("ALTER TABLE sessions ADD COLUMN is_baseline INTEGER DEFAULT 0")
        conn.commit()


def flag_by_id(conn: sqlite3.Connection, session_id: int, value: int = 1) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT id, file_path FROM sessions WHERE id = ?", (session_id,))
    row = cur.fetchone()
    if not row:
        print(f"No session with id {session_id}")
        return False
    cur.execute("UPDATE sessions SET is_baseline = ? WHERE id = ?", (value, session_id))
    conn.commit()
    action = "Flagged" if value else "Unflagged"
    print(f"{action} session {session_id}: {row[1]}")
    return True


def flag_by_date_track(conn: sqlite3.Connection, date: str, track: str, value: int = 1) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, file_path FROM sessions WHERE file_path LIKE ? AND file_path LIKE ?",
        (f"%{track}%", f"%{date}%"),
    )
    rows = cur.fetchall()
    if not rows:
        print(f"No sessions found for track '{track}' on {date}")
        return False
    action = "Flagged" if value else "Unflagged"
    for sid, fpath in rows:
        cur.execute("UPDATE sessions SET is_baseline = ? WHERE id = ?", (value, sid))
        print(f"{action} session {sid}: {fpath}")
    conn.commit()
    return True


def list_baselines(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(sessions)")
    if not any(row[1] == "is_baseline" for row in cur.fetchall()):
        print("No is_baseline column yet. Run the pipeline first.")
        return
    cur.execute(
        "SELECT id, file_path, track_name FROM sessions WHERE is_baseline = 1 ORDER BY id"
    )
    rows = cur.fetchall()
    if not rows:
        print("No baseline sessions flagged.")
        return
    print(f"{'ID':>5}  {'Track':<25}  File")
    print("-" * 70)
    for sid, fpath, track in rows:
        print(f"{sid:>5}  {(track or '?'):<25}  {Path(fpath).name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Flag sessions as baseline runs")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--session-id", type=int, help="Session ID to flag")
    parser.add_argument("--date", help="Date (YYYY-MM-DD) to match sessions")
    parser.add_argument("--track", help="Track name substring to match (e.g. 'monza full')")
    parser.add_argument("--list", action="store_true", help="List current baselines")
    parser.add_argument("--unflag", action="store_true", help="Remove baseline flag instead of setting it")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    _ensure_column(conn)

    if args.list:
        list_baselines(conn)
    elif args.session_id:
        flag_by_id(conn, args.session_id, 0 if args.unflag else 1)
    elif args.date and args.track:
        flag_by_date_track(conn, args.date, args.track, 0 if args.unflag else 1)
    else:
        parser.print_help()
        print("\nExamples:")
        print('  python3 scripts/flag_baseline.py --db data/telemetry.db --session-id 245')
        print('  python3 scripts/flag_baseline.py --db data/telemetry.db --date 2026-02-04 --track "monza full"')
        print('  python3 scripts/flag_baseline.py --db data/telemetry.db --list')
        print('  python3 scripts/flag_baseline.py --db data/telemetry.db --session-id 245 --unflag')

    conn.close()


if __name__ == "__main__":
    main()
