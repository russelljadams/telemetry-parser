from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Sequence

from .incident_detection import (
    IncidentEvent,
    event_counts_by_lap,
    hotspot_buckets,
    summarize_events,
)
from .track_config import load_track_config, tag_zone
from .metrics import LapMetrics
from .segments import LapSegment


def _format_seconds(value: float) -> str:
    if value <= 0:
        return "-"
    minutes = int(value // 60)
    seconds = value % 60
    return f"{minutes}:{seconds:06.3f}" if minutes else f"{seconds:.3f}s"


def _extract_session_metadata(session_info: Optional[str]) -> Dict[str, str]:
    if not session_info:
        return {}
    keys = {
        "TrackDisplayName": "track",
        "TrackName": "track_id",
        "CarScreenName": "car",
        "CarClassShortName": "car_class",
        "SessionType": "session_type",
    }
    out: Dict[str, str] = {}
    for line in session_info.splitlines():
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip()
        if key in keys:
            out[keys[key]] = val.strip()
        if len(out) == len(keys):
            break
    return out


def write_session_report(
    output_path: str,
    file_path: str,
    metrics: LapMetrics,
    segments: Sequence[LapSegment],
    incidents_by_lap: Dict[int, int],
    session_info: Optional[str] = None,
    events: Optional[Sequence[IncidentEvent]] = None,
    lap_dist_pct: Optional[Sequence[float]] = None,
) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    meta = _extract_session_metadata(session_info)

    complete_laps = [seg for seg in segments if seg.is_complete]
    resets = sum(1 for seg in segments if seg.is_reset)
    total_incidents = sum(incidents_by_lap.values())
    event_summary = summarize_events(events) if events else {}
    event_counts = event_counts_by_lap(events) if events else {}
    hotspots = hotspot_buckets(events, lap_dist_pct) if events and lap_dist_pct else []
    track_config = load_track_config(meta.get("track_id") if meta else None)
    zone_counts: Dict[str, int] = {}
    if events and lap_dist_pct and track_config and track_config.zones:
        for event in events:
            if event.index < 0 or event.index >= len(lap_dist_pct):
                continue
            zone = tag_zone(float(lap_dist_pct[event.index]), track_config.zones)
            if zone:
                zone_counts[zone] = zone_counts.get(zone, 0) + 1

    lines = []
    lines.append("# Session Report")
    lines.append("")
    lines.append(f"Generated: {datetime.utcnow().isoformat()}Z")
    lines.append(f"Source file: {file_path}")
    if meta:
        lines.append("")
        lines.append("## Session Metadata")
        lines.append("")
        for label in ["track", "track_id", "car", "car_class", "session_type"]:
            if label in meta:
                lines.append(f"- {label.replace('_', ' ').title()}: {meta[label]}")

    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total segments: {len(segments)}")
    lines.append(f"- Complete laps: {len(complete_laps)}")
    lines.append(f"- Active resets: {resets}")
    lines.append(f"- Total incidents: {total_incidents}")
    if event_summary:
        for key, val in event_summary.items():
            lines.append(f"- {key.replace('_', ' ').title()} events: {val}")

    lines.append("")
    lines.append("## Lap Metrics")
    lines.append("")
    lines.append(f"- Best lap: {_format_seconds(metrics.best_lap)}")
    lines.append(f"- Median lap: {_format_seconds(metrics.median_lap)}")
    lines.append(f"- Worst lap: {_format_seconds(metrics.worst_lap)}")
    lines.append(f"- Std dev: {_format_seconds(metrics.stddev_lap)}")
    lines.append(f"- IQR: {_format_seconds(metrics.iqr_lap)}")

    if events:
        lines.append("")
        lines.append("## Error Events")
        lines.append("")
        lines.append("Type | Time | Lap")
        lines.append("--- | --- | ---")
        for event in events:
            lines.append(
                f"{event.event_type} | {_format_seconds(event.session_time)} | {event.lap_number}"
            )

    if hotspots:
        lines.append("")
        lines.append("## Hotspots (LapDistPct bins)")
        lines.append("")
        lines.append("LapDistPct | Count")
        lines.append("--- | ---")
        for start, end, count in hotspots[:6]:
            lines.append(f"{start:.2f}-{end:.2f} | {count}")

    if zone_counts:
        lines.append("")
        lines.append("## Hotspots (Tagged Zones)")
        lines.append("")
        lines.append("Zone | Count")
        lines.append("--- | ---")
        for zone, count in sorted(zone_counts.items(), key=lambda item: item[1], reverse=True):
            lines.append(f"{zone} | {count}")

    lines.append("")
    lines.append("## Lap Breakdown")
    lines.append("")
    lines.append("Lap | Time | Complete | Reset | Incidents | Error Events")
    lines.append("--- | --- | --- | --- | --- | ---")
    for seg in segments:
        lap_time = _format_seconds(seg.lap_time)
        inc = incidents_by_lap.get(seg.lap_number, 0)
        event_count = event_counts.get(seg.lap_number, 0)
        lines.append(
            f"{seg.lap_number} | {lap_time} | {int(seg.is_complete)} | {int(seg.is_reset)} | {inc} | {event_count}"
        )

    output.write_text("\n".join(lines), encoding="utf-8")


def write_publishable_summary(
    output_path: str,
    file_path: str,
    metrics: LapMetrics,
    segments: Sequence[LapSegment],
    incidents_by_lap: Dict[int, int],
    session_info: Optional[str] = None,
    events: Optional[Sequence[IncidentEvent]] = None,
    lap_dist_pct: Optional[Sequence[float]] = None,
) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    meta = _extract_session_metadata(session_info)
    complete_laps = [seg for seg in segments if seg.is_complete]
    resets = sum(1 for seg in segments if seg.is_reset)
    total_incidents = sum(incidents_by_lap.values())
    event_summary = summarize_events(events) if events else {}
    hotspots = hotspot_buckets(events, lap_dist_pct) if events and lap_dist_pct else []
    track_config = load_track_config(meta.get("track_id") if meta else None)
    zone_counts: Dict[str, int] = {}
    if events and lap_dist_pct and track_config and track_config.zones:
        for event in events:
            if event.index < 0 or event.index >= len(lap_dist_pct):
                continue
            zone = tag_zone(float(lap_dist_pct[event.index]), track_config.zones)
            if zone:
                zone_counts[zone] = zone_counts.get(zone, 0) + 1

    lines = []
    lines.append("# Session Summary")
    lines.append("")
    lines.append(f"Generated: {datetime.utcnow().isoformat()}Z")
    lines.append(f"Source file: {file_path}")
    lines.append("")
    lines.append("## Snapshot")
    lines.append("")
    if meta:
        lines.append(f"- Track: {meta.get('track', '')} ({meta.get('track_id', '')})")
        lines.append(f"- Car: {meta.get('car', '')}")
    lines.append(f"- Complete laps: {len(complete_laps)}")
    lines.append(f"- Active resets: {resets}")
    lines.append(f"- Total incidents: {total_incidents}")
    lines.append(f"- Best lap: {_format_seconds(metrics.best_lap)}")
    lines.append(f"- Median lap: {_format_seconds(metrics.median_lap)}")
    lines.append(f"- Std dev: {_format_seconds(metrics.stddev_lap)}")

    if event_summary:
        lines.append("")
        lines.append("## Error Events")
        lines.append("")
        for key, val in event_summary.items():
            lines.append(f"- {key.replace('_', ' ').title()}: {val}")

    if zone_counts:
        lines.append("")
        lines.append("## Hotspots")
        lines.append("")
        for zone, count in sorted(zone_counts.items(), key=lambda item: item[1], reverse=True):
            lines.append(f"- {zone}: {count}")
    elif hotspots:
        lines.append("")
        lines.append("## Hotspots")
        lines.append("")
        for start, end, count in hotspots[:6]:
            lines.append(f"- {start:.2f}-{end:.2f}: {count}")

    output.write_text("\n".join(lines), encoding="utf-8")
