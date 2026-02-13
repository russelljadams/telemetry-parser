from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List, Optional, Sequence

from .incident_detection import IncidentEvent, event_counts_by_lap, serious_event_counts_by_lap
from .metrics import CleanMetrics, LapMetrics, is_valid_lap
from .segments import LapSegment, ResetEvent


def connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL,
            start_time INTEGER,
            session_start_time REAL,
            session_end_time REAL,
            session_lap_count INTEGER,
            record_count INTEGER,
            best_lap REAL,
            median_lap REAL,
            worst_lap REAL,
            stddev_lap REAL,
            iqr_lap REAL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS laps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            lap_number INTEGER,
            start_time REAL,
            end_time REAL,
            lap_time REAL,
            is_complete INTEGER,
            is_reset INTEGER,
            incidents INTEGER,
            FOREIGN KEY(session_id) REFERENCES sessions(id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            session_time REAL,
            lap_number INTEGER,
            index_in_session INTEGER,
            FOREIGN KEY(session_id) REFERENCES sessions(id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reset_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            lap_number INTEGER,
            lap_dist_pct REAL,
            index_in_session INTEGER,
            FOREIGN KEY(session_id) REFERENCES sessions(id)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sector_times (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            lap_id INTEGER,
            lap_number INTEGER,
            sector_name TEXT NOT NULL,
            sector_time REAL,
            FOREIGN KEY(session_id) REFERENCES sessions(id),
            FOREIGN KEY(lap_id) REFERENCES laps(id)
        );
        """
    )

    # Migrations for new columns (idempotent)
    migrations = [
        "ALTER TABLE laps ADD COLUMN is_clean INTEGER DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN track_name TEXT",
        "ALTER TABLE sessions ADD COLUMN car_name TEXT",
        "ALTER TABLE sessions ADD COLUMN clean_best_lap REAL",
        "ALTER TABLE sessions ADD COLUMN clean_median_lap REAL",
        "ALTER TABLE sessions ADD COLUMN clean_stddev_lap REAL",
        "ALTER TABLE sessions ADD COLUMN clean_lap_count INTEGER",
        "ALTER TABLE sessions ADD COLUMN classified_session_type TEXT",
        "ALTER TABLE laps ADD COLUMN has_official_time INTEGER DEFAULT 1",
        "ALTER TABLE laps ADD COLUMN event_count INTEGER DEFAULT 0",
        "ALTER TABLE sessions ADD COLUMN is_baseline INTEGER DEFAULT 0",
    ]
    for sql in migrations:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists

    conn.commit()


def insert_session(
    conn: sqlite3.Connection,
    file_path: str,
    disk_header,
    metrics: LapMetrics,
    segments: Iterable[LapSegment],
    incidents_by_lap,
    events: Optional[Sequence[IncidentEvent]] = None,
    reset_events: Optional[Sequence[ResetEvent]] = None,
    track_name: Optional[str] = None,
    car_name: Optional[str] = None,
    clean_metrics: Optional[CleanMetrics] = None,
    classified_session_type: Optional[str] = None,
    min_valid_lap_time: float = 0.0,
    max_valid_lap_time: float = 0.0,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sessions (
            file_path, start_time, session_start_time, session_end_time,
            session_lap_count, record_count, best_lap, median_lap,
            worst_lap, stddev_lap, iqr_lap,
            track_name, car_name,
            clean_best_lap, clean_median_lap, clean_stddev_lap, clean_lap_count,
            classified_session_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            file_path,
            getattr(disk_header, "start_time", None),
            getattr(disk_header, "session_start_time", None),
            getattr(disk_header, "session_end_time", None),
            getattr(disk_header, "session_lap_count", None),
            getattr(disk_header, "record_count", None),
            metrics.best_lap,
            metrics.median_lap,
            metrics.worst_lap,
            metrics.stddev_lap,
            metrics.iqr_lap,
            track_name,
            car_name,
            clean_metrics.clean_best_lap if clean_metrics else None,
            clean_metrics.clean_median_lap if clean_metrics else None,
            clean_metrics.clean_stddev_lap if clean_metrics else None,
            clean_metrics.clean_lap_count if clean_metrics else None,
            classified_session_type,
        ),
    )
    session_id = cur.lastrowid

    segments_list = list(segments)
    lap_id_map: Dict[int, int] = {}

    # Count all events per lap (for informational tracking)
    all_events_per_lap: Dict[int, int] = {}
    # Count only serious events (spin, off_track) for clean determination
    serious_per_lap: Dict[int, int] = {}
    if events:
        all_events_per_lap = event_counts_by_lap(events)
        serious_per_lap = serious_event_counts_by_lap(events)

    for seg in segments_list:
        inc = incidents_by_lap.get(seg.lap_number, 0)
        evt = all_events_per_lap.get(seg.lap_number, 0)
        # A lap is "clean" = a real, complete lap the driver actually drove.
        # Incidents and events are tracked but do NOT exclude a lap —
        # they're part of the driver's real pace and belong in variance metrics.
        # Only structural issues exclude: incomplete, reset, untimed, AFK.
        clean = 1 if is_valid_lap(seg, min_valid_lap_time, max_valid_lap_time) else 0
        cur.execute(
            """
            INSERT INTO laps (
                session_id, lap_number, start_time, end_time, lap_time,
                is_complete, is_reset, incidents, is_clean, has_official_time,
                event_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                seg.lap_number,
                seg.start_time,
                seg.end_time,
                seg.lap_time,
                1 if seg.is_complete else 0,
                1 if seg.is_reset else 0,
                inc,
                clean,
                1 if seg.has_official_time else 0,
                evt,
            ),
        )
        lap_id_map[seg.lap_number] = cur.lastrowid

    if events:
        for event in events:
            cur.execute(
                """
                INSERT INTO events (
                    session_id, event_type, session_time, lap_number, index_in_session
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    event.event_type,
                    event.session_time,
                    event.lap_number,
                    event.index,
                ),
            )

    if reset_events:
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
    return session_id


def insert_sector_times(
    conn: sqlite3.Connection,
    session_id: int,
    sector_data: Sequence[Dict],
    lap_id_map: Optional[Dict[int, int]] = None,
) -> None:
    cur = conn.cursor()
    for entry in sector_data:
        lap_number = entry.get("lap_number")
        lap_id = lap_id_map.get(lap_number) if lap_id_map else None
        cur.execute(
            """
            INSERT INTO sector_times (
                session_id, lap_id, lap_number, sector_name, sector_time
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                session_id,
                lap_id,
                lap_number,
                entry["sector_name"],
                entry["sector_time"],
            ),
        )
    conn.commit()


def get_lap_id_map(conn: sqlite3.Connection, session_id: int) -> Dict[int, int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, lap_number FROM laps WHERE session_id = ?",
        (session_id,),
    )
    return {row[1]: row[0] for row in cur.fetchall()}
