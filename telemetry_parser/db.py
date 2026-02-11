from __future__ import annotations

import sqlite3
from typing import Iterable, List, Optional, Sequence

from .incident_detection import IncidentEvent
from .metrics import LapMetrics
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
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO sessions (
            file_path, start_time, session_start_time, session_end_time,
            session_lap_count, record_count, best_lap, median_lap,
            worst_lap, stddev_lap, iqr_lap
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        ),
    )
    session_id = cur.lastrowid

    for seg in segments:
        cur.execute(
            """
            INSERT INTO laps (
                session_id, lap_number, start_time, end_time, lap_time,
                is_complete, is_reset, incidents
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                seg.lap_number,
                seg.start_time,
                seg.end_time,
                seg.lap_time,
                1 if seg.is_complete else 0,
                1 if seg.is_reset else 0,
                incidents_by_lap.get(seg.lap_number, 0),
            ),
        )

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
