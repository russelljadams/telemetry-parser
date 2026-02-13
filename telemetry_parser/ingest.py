from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .db import connect, get_lap_id_map, init_db, insert_sector_times, insert_session
from .ibt import IBTReader
from .incident_detection import detect_events
from .metrics import (
    compute_clean_metrics,
    compute_lap_metrics,
    incident_counts,
    override_best_lap,
)
from .reporting import write_publishable_summary, write_session_report
from .segments import detect_reset_events, segment_laps
from .track_config import get_max_valid_lap_time, get_min_valid_lap_time

_ALL_CHANNELS = list(dict.fromkeys([
    "SessionTime",
    "Lap",
    "LapDistPct",
    "LapLastLapTime",
    "LapCompleted",
    "PlayerIncidents",
    "LapBestLapTime",
    "Speed",
    "YawRate",
    "SteeringWheelAngle",
    "IsOnTrack",
]))

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

_FILENAME_RE = re.compile(
    r"superformulalights324_(?P<track>.+?) \d{4}-\d{2}-\d{2}",
    re.IGNORECASE,
)


def _read_channels(reader: IBTReader, names: List[str]) -> Dict[str, List[object]]:
    data = {name: [] for name in names}
    for record in reader.iter_records(names):
        for name in names:
            data[name].append(record[name])
    return data


def _extract_track_id(file_path: str, session_info: Optional[str] = None) -> Optional[str]:
    if session_info:
        for line in session_info.splitlines():
            if "TrackName" in line and ":" in line:
                _, val = line.split(":", 1)
                name = val.strip()
                if name:
                    return name
    match = _FILENAME_RE.search(Path(file_path).name)
    if match:
        return match.group("track").strip()
    return None


def _extract_metadata(session_info: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    track_name = None
    car_name = None
    if not session_info:
        return track_name, car_name
    for line in session_info.splitlines():
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip()
        if key == "TrackDisplayName" and not track_name:
            track_name = val.strip()
        elif key == "CarScreenName" and not car_name:
            car_name = val.strip()
        if track_name and car_name:
            break
    return track_name, car_name


def ingest_file(file_path: str, db_path: str, report_dir: str, summary_dir: str) -> int:
    reader = IBTReader(file_path).read()
    missing = [name for name in REQUIRED_CHANNELS if name not in reader.var_by_name]
    if missing:
        raise ValueError(f"Missing required channels: {', '.join(missing)}")

    # Read all available channels in a single pass
    available = [ch for ch in _ALL_CHANNELS if ch in reader.var_by_name]
    channels = _read_channels(reader, available)

    segments = segment_laps(
        session_time=channels["SessionTime"],
        lap=channels["Lap"],
        lap_dist_pct=channels["LapDistPct"],
        lap_last_lap_time=channels["LapLastLapTime"],
        lap_completed=channels["LapCompleted"],
    )
    reset_events = detect_reset_events(
        lap=channels["Lap"],
        lap_dist_pct=channels["LapDistPct"],
        session_time=channels["SessionTime"],
    )

    # Determine track and valid lap time range
    track_id = _extract_track_id(file_path, reader.session_info)
    min_valid_lap_time = get_min_valid_lap_time(track_id)
    max_valid_lap_time = get_max_valid_lap_time(track_id)

    metrics = compute_lap_metrics(segments, min_valid_lap_time=min_valid_lap_time, max_valid_lap_time=max_valid_lap_time)

    if "LapBestLapTime" in channels:
        best_candidates = [v for v in channels["LapBestLapTime"] if v and v > 0]
        if best_candidates:
            metrics = override_best_lap(metrics, min(best_candidates))
    incidents_by_lap = incident_counts(channels["PlayerIncidents"], segments)

    events = None
    event_lap_dist_pct = None
    events_by_lap: Dict[int, int] = {}
    if all(name in channels for name in EVENT_CHANNELS):
        events = detect_events(
            session_time=channels["SessionTime"],
            lap=channels["Lap"],
            speed=channels["Speed"],
            yaw_rate=channels["YawRate"],
            steering_angle=channels["SteeringWheelAngle"],
            is_on_track=channels["IsOnTrack"],
        )
        event_lap_dist_pct = channels["LapDistPct"]
        from .incident_detection import serious_event_counts_by_lap
        events_by_lap = serious_event_counts_by_lap(events)

    # Clean metrics consider incidents + serious events (telemetry-based filtering)
    # big_save is informational only — not used for clean determination
    clean_metrics = compute_clean_metrics(
        segments,
        min_valid_lap_time=min_valid_lap_time,
        max_valid_lap_time=max_valid_lap_time,
        incidents_by_lap=incidents_by_lap,
        events_by_lap=events_by_lap,
    )

    # Extract display metadata
    track_name, car_name = _extract_metadata(reader.session_info)

    # Classify session (imported lazily to avoid circular import before classification module exists)
    classified_session_type = None
    try:
        from .classification import classify_session
        classified_session_type = classify_session(
            segments, reset_events, min_valid_lap_time,
        )
    except ImportError:
        pass

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
        reset_events=reset_events,
        track_name=track_name,
        car_name=car_name,
        clean_metrics=clean_metrics,
        classified_session_type=classified_session_type,
        min_valid_lap_time=min_valid_lap_time,
        max_valid_lap_time=max_valid_lap_time,
    )

    # Sector timing
    try:
        from .sectors import compute_sector_times
        from .track_config import load_track_config

        track_config = load_track_config(track_id)
        if track_config and track_config.zones:
            sector_data = compute_sector_times(
                session_time=channels["SessionTime"],
                lap_dist_pct=channels["LapDistPct"],
                segments=segments,
                zones=track_config.zones,
            )
            if sector_data:
                lap_id_map = get_lap_id_map(conn, session_id)
                insert_sector_times(conn, session_id, sector_data, lap_id_map)
    except ImportError:
        pass

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
