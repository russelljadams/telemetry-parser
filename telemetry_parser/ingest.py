from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

from .db import connect, init_db, insert_session
from .ibt import IBTReader
from .incident_detection import detect_events
from .metrics import compute_lap_metrics, incident_counts, override_best_lap
from .reporting import write_publishable_summary, write_session_report
from .segments import segment_laps

REQUIRED_CHANNELS = [
    "SessionTime",
    "Lap",
    "LapDistPct",
    "LapLastLapTime",
    "LapCompleted",
    "PlayerIncidents",
    "LapBestLapTime",
]

EVENT_CHANNELS = [
    "SessionTime",
    "Lap",
    "LapDistPct",
    "Speed",
    "YawRate",
    "SteeringWheelAngle",
    "IsOnTrack",
]


def _read_channels(reader: IBTReader, names: List[str]) -> Dict[str, List[object]]:
    data = {name: [] for name in names}
    for record in reader.iter_records(names):
        for name in names:
            data[name].append(record[name])
    return data


def ingest_file(file_path: str, db_path: str, report_dir: str, summary_dir: str) -> int:
    reader = IBTReader(file_path).read()
    missing = [name for name in REQUIRED_CHANNELS if name not in reader.var_by_name]
    if missing:
        raise ValueError(f"Missing required channels: {', '.join(missing)}")

    channels = _read_channels(reader, REQUIRED_CHANNELS)

    segments = segment_laps(
        session_time=channels["SessionTime"],
        lap=channels["Lap"],
        lap_dist_pct=channels["LapDistPct"],
        lap_last_lap_time=channels["LapLastLapTime"],
        lap_completed=channels["LapCompleted"],
    )

    metrics = compute_lap_metrics(segments)
    if "LapBestLapTime" in channels:
        best_candidates = [v for v in channels["LapBestLapTime"] if v and v > 0]
        if best_candidates:
            metrics = override_best_lap(metrics, min(best_candidates))
    incidents_by_lap = incident_counts(channels["PlayerIncidents"], segments)

    events = None
    event_lap_dist_pct = None
    if all(name in reader.var_by_name for name in EVENT_CHANNELS):
        event_channels = _read_channels(reader, EVENT_CHANNELS)
        events = detect_events(
            session_time=event_channels["SessionTime"],
            lap=event_channels["Lap"],
            speed=event_channels["Speed"],
            yaw_rate=event_channels["YawRate"],
            steering_angle=event_channels["SteeringWheelAngle"],
            is_on_track=event_channels["IsOnTrack"],
        )
        event_lap_dist_pct = event_channels["LapDistPct"]

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    init_db(conn)
    session_id = insert_session(
        conn,
        file_path=file_path,
        disk_header=reader.disk_header,
        metrics=metrics,
        segments=segments,
        incidents_by_lap=incidents_by_lap,
        events=events,
    )
    conn.close()

    report_path = Path(report_dir) / f"session_{session_id}.md"
    write_session_report(
        output_path=str(report_path),
        file_path=file_path,
        metrics=metrics,
        segments=segments,
        incidents_by_lap=incidents_by_lap,
        session_info=reader.session_info,
        events=events,
        lap_dist_pct=event_lap_dist_pct,
    )

    summary_path = Path(summary_dir) / f"session_{session_id}.md"
    write_publishable_summary(
        output_path=str(summary_path),
        file_path=file_path,
        metrics=metrics,
        segments=segments,
        incidents_by_lap=incidents_by_lap,
        session_info=reader.session_info,
        events=events,
        lap_dist_pct=event_lap_dist_pct,
    )

    return session_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest an iRacing IBT file")
    parser.add_argument("ibt_path", help="Path to .ibt file")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--reports", default="reports", help="Reports output directory")
    parser.add_argument("--summaries", default="summaries", help="Publishable summaries output directory")
    args = parser.parse_args()

    session_id = ingest_file(args.ibt_path, args.db, args.reports, args.summaries)
    print(f"Ingested session {session_id}")


if __name__ == "__main__":
    main()
