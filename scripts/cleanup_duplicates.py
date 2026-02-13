#!/usr/bin/env python3
"""Remove duplicate sessions (same file_path) and add unique index."""
from __future__ import annotations

import argparse
import sqlite3


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove duplicate sessions by file_path")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without modifying DB")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    # Find duplicates
    cur.execute(
        """
        SELECT file_path, COUNT(*) as cnt, GROUP_CONCAT(id) as ids
        FROM sessions
        GROUP BY file_path
        HAVING cnt > 1
        """
    )
    duplicates = cur.fetchall()

    if not duplicates:
        print("No duplicate sessions found.")
    else:
        print(f"Found {len(duplicates)} file paths with duplicates.")

    total_removed = 0
    for file_path, count, ids_str in duplicates:
        ids = sorted(int(x) for x in ids_str.split(","))
        keep_id = ids[0]
        remove_ids = ids[1:]
        print(f"  {file_path}: keeping session {keep_id}, removing {remove_ids}")

        if not args.dry_run:
            for remove_id in remove_ids:
                cur.execute("DELETE FROM laps WHERE session_id = ?", (remove_id,))
                cur.execute("DELETE FROM events WHERE session_id = ?", (remove_id,))
                cur.execute("DELETE FROM reset_events WHERE session_id = ?", (remove_id,))
                # Clean sector_times if table exists
                try:
                    cur.execute("DELETE FROM sector_times WHERE session_id = ?", (remove_id,))
                except sqlite3.OperationalError:
                    pass
                cur.execute("DELETE FROM sessions WHERE id = ?", (remove_id,))
                total_removed += 1

    if not args.dry_run and total_removed > 0:
        conn.commit()
        print(f"Removed {total_removed} duplicate sessions.")

    # Add unique index (idempotent)
    if not args.dry_run:
        try:
            cur.execute(
                "CREATE UNIQUE INDEX idx_sessions_file_path ON sessions(file_path)"
            )
            conn.commit()
            print("Created unique index on sessions(file_path).")
        except sqlite3.OperationalError:
            print("Unique index on sessions(file_path) already exists.")

    conn.close()


if __name__ == "__main__":
    main()
