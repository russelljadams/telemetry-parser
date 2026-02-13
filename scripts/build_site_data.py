#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from statistics import median, pstdev
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from telemetry_parser.track_config import DEFAULT_MAX_TIMES, DEFAULT_MIN_TIMES, GLOBAL_MAX_LAP_TIME


# Porsche 911 GT3 Cup — iRacing car ID is "porsche9922cup"
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
TRACK_NAME_MAP = {
    "spa 2024 up": "spa",
    "monza full": "monza full",
    # Confirm these once first IBT files are generated:
    # "nurburgring grand prix": "nurburgring gp",
    # "circuit de barcelona": "barcelona gp",
}


def _percentile(sorted_vals: Sequence[float], pct: float) -> float:
    """Linear interpolation percentile (copied from metrics.py)."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def normalize_track(raw_track: str) -> str:
    """Map iRacing track names to experiment track IDs."""
    lower = raw_track.lower().strip()
    return TRACK_NAME_MAP.get(lower, raw_track)


def parse_file_metadata(file_path: str) -> Tuple[str, str, bool]:
    name = Path(file_path).name

    # Porsche 911 GT3 Cup pattern only — SFL data is archived, not exported
    match = FILENAME_RE.search(name)
    if match:
        track = normalize_track(match.group("track").strip())
        date = match.group("date")
        return track, date, True

    # Fallback: baseline filename pattern (Porsche only — reject SFL baselines)
    bmatch = BASELINE_RE.search(name)
    if bmatch and "porsche9922cup" in name.lower():
        track_key = bmatch.group("track").upper()
        track = BASELINE_TRACK_MAP.get(track_key)
        if track:
            month = int(bmatch.group("month"))
            day = int(bmatch.group("day"))
            year = int(bmatch.group("year"))
            if year < 100:
                year += 2000
            date_str = f"{year:04d}-{month:02d}-{day:02d}"
            return track, date_str, True

    return "unknown", "unknown", False


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


def _has_column(cur: sqlite3.Cursor, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def _has_table(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _add_rolling_averages(series: List[Dict], window_days: int = 7) -> None:
    """Add 7-day rolling averages to a daily timeseries in-place."""
    if not series:
        return
    for i, point in enumerate(series):
        current_date = datetime.strptime(point["date"], "%Y-%m-%d")
        window_start = current_date - timedelta(days=window_days - 1)

        window_best: List[float] = []
        window_median: List[float] = []
        window_stddev: List[float] = []

        for j in range(max(0, i - window_days * 2), i + 1):
            jdate = datetime.strptime(series[j]["date"], "%Y-%m-%d")
            if window_start <= jdate <= current_date:
                window_best.append(series[j]["bestLap"])
                window_median.append(series[j]["medianLap"])
                window_stddev.append(series[j]["stdDev"])

        if window_best:
            point["bestLap7d"] = round(sum(window_best) / len(window_best), 3)
            point["medianLap7d"] = round(sum(window_median) / len(window_median), 3)
            point["stdDev7d"] = round(sum(window_stddev) / len(window_stddev), 3)


def _iso_week(date_str: str) -> str:
    d = datetime.strptime(date_str, "%Y-%m-%d")
    monday = d - timedelta(days=d.weekday())
    return monday.strftime("%Y-%m-%d")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build JSON data for the website")
    parser.add_argument("--db", default="data/telemetry.db", help="SQLite database path")
    parser.add_argument("--output", default="../russelljadams/public/data", help="Output folder for JSON data")
    args = parser.parse_args()

    db_path = Path(args.db)
    output_root = Path(args.output).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    script_dir = Path(__file__).resolve().parent
    tracks_config_dir = script_dir.parent / "tracks"

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

    has_clean = _has_column(cur, "laps", "is_clean")
    has_classified = _has_column(cur, "sessions", "classified_session_type")
    has_baseline = _has_column(cur, "sessions", "is_baseline")
    has_sector_table = _has_table(cur, "sector_times")

    # ── Load all sessions ──────────────────────────────────────────
    cur.execute(
        """
        SELECT id, file_path, session_start_time, session_end_time, session_lap_count
        FROM sessions
        """
    )
    sessions = cur.fetchall()

    cur.execute("SELECT session_id, lap_dist_pct FROM reset_events")
    reset_rows = cur.fetchall()

    resets_by_session: Dict[int, List[float]] = defaultdict(list)
    for session_id, lap_dist_pct in reset_rows:
        resets_by_session[int(session_id)].append(float(lap_dist_pct))

    if has_clean:
        cur.execute(
            """
            SELECT session_id,
                   SUM(CASE WHEN is_clean = 1 THEN 1 ELSE 0 END) as clean_laps,
                   SUM(CASE WHEN is_reset = 1 THEN 1 ELSE 0 END) as reset_count
            FROM laps
            GROUP BY session_id
            """
        )
    else:
        cur.execute(
            """
            SELECT session_id,
                   SUM(CASE WHEN is_complete = 1 AND is_reset = 0 THEN 1 ELSE 0 END) as clean_laps,
                   SUM(CASE WHEN is_reset = 1 THEN 1 ELSE 0 END) as reset_count
            FROM laps
            GROUP BY session_id
            """
        )
    lap_counts_by_session: Dict[int, Tuple[int, int]] = {}
    for sid, clean_laps, reset_count in cur.fetchall():
        lap_counts_by_session[int(sid)] = (int(clean_laps), int(reset_count))

    total_sessions = 0
    total_laps = 0
    total_duration_s = 0.0
    total_resets = 0

    monthly_accum: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"sessions": 0, "duration_s": 0.0, "laps": 0}
    )

    # overall_bins computed after session loop (only valid sessions)
    valid_reset_values: List[float] = []

    daily_data: Dict[str, Dict] = {}
    track_data: Dict[str, Dict] = {}

    session_track_date: Dict[int, Tuple[str, str]] = {}
    session_classified_type: Dict[int, Optional[str]] = {}

    if has_classified:
        cur.execute("SELECT id, classified_session_type FROM sessions")
        for sid, stype in cur.fetchall():
            session_classified_type[int(sid)] = stype

    for session_id, file_path, start_time, end_time, lap_count in sessions:
        track, date_str, is_valid = parse_file_metadata(file_path)
        if not is_valid:
            continue
        if date_str < "2026-01-01" or date_str > "2026-12-31":
            continue
        duration_s = (end_time or 0) - (start_time or 0)
        clean_laps, reset_count = lap_counts_by_session.get(int(session_id), (0, 0))
        resets = resets_by_session.get(int(session_id), [])
        total_sessions += 1
        total_laps += clean_laps
        total_duration_s += duration_s
        total_resets += len(resets)

        session_track_date[int(session_id)] = (track, date_str)

        month_key = date_str[:7]
        monthly_accum[month_key]["sessions"] += 1
        monthly_accum[month_key]["duration_s"] += duration_s
        monthly_accum[month_key]["laps"] += clean_laps

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
        day["laps"] += clean_laps
        day["durationSeconds"] += duration_s
        day["resets"] += len(resets)

        track_entry = day["tracks"][track]
        track_entry["sessions"] += 1
        track_entry["laps"] += clean_laps
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
        track_root["laps"] += clean_laps
        track_root["durationSeconds"] += duration_s
        track_root["resets"] += len(resets)

        track_root.setdefault("_reset_values", []).extend(resets)
        day.setdefault("_reset_values", []).extend(resets)
        track_entry.setdefault("_reset_values", []).extend(resets)
        valid_reset_values.extend(resets)

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

    # ── Build resets-by-(track, day) lookup from daily_data ───────
    resets_by_track_day: Dict[Tuple[str, str], int] = {}
    for date_str, day in daily_data.items():
        for track_name, track_entry in day.get("tracks", {}).items():
            resets_by_track_day[(track_name, date_str)] = track_entry.get("resets", 0)

    # ── Track daily time-series (enhanced with p25/p75/iqr) ────────
    if has_clean:
        cur.execute(
            "SELECT session_id, lap_time FROM laps WHERE is_clean = 1"
        )
    else:
        cur.execute(
            """
            SELECT session_id, lap_time
            FROM laps
            WHERE is_complete = 1 AND is_reset = 0 AND lap_time > 0
            """
        )
    lap_rows = cur.fetchall()
    lap_times_by_track_day: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for session_id, lap_time in lap_rows:
        key = session_track_date.get(int(session_id))
        if not key:
            continue
        track, date_str = key
        t = float(lap_time)
        if not has_clean:
            min_time = DEFAULT_MIN_TIMES.get(track, 30.0)
            max_time = DEFAULT_MAX_TIMES.get(track, GLOBAL_MAX_LAP_TIME)
            if t < min_time or t > max_time:
                continue
        lap_times_by_track_day[(track, date_str)].append(t)

    track_timeseries: Dict[str, List[Dict]] = defaultdict(list)
    for (track, date_str), times in sorted(lap_times_by_track_day.items()):
        if not times:
            continue
        sorted_times = sorted(times)
        best = sorted_times[0]
        worst = sorted_times[-1]
        med = median(sorted_times)
        stddev = pstdev(sorted_times) if len(sorted_times) > 1 else 0.0
        p25 = _percentile(sorted_times, 0.25)
        p75 = _percentile(sorted_times, 0.75)
        completed = len(times)
        resets = resets_by_track_day.get((track, date_str), 0)
        attempts = completed + resets
        clean_rate = round(completed / attempts, 3) if attempts > 0 else 0.0
        track_timeseries[track].append(
            {
                "date": date_str,
                "bestLap": round(best, 3),
                "medianLap": round(med, 3),
                "stdDev": round(stddev, 3),
                "completeLaps": completed,
                "resets": resets,
                "cleanRate": clean_rate,
                "worstLap": round(worst, 3),
                "p25": round(p25, 3),
                "p75": round(p75, 3),
                "iqr": round(p75 - p25, 3),
            }
        )

    # Add cumulative hours per track
    # Build duration lookup: (track, date) -> seconds
    duration_by_track_day: Dict[Tuple[str, str], float] = {}
    for date_str, day in daily_data.items():
        for track_name, track_entry in day.get("tracks", {}).items():
            duration_by_track_day[(track_name, date_str)] = track_entry.get("durationSeconds", 0.0)

    for track, points in track_timeseries.items():
        cumulative = 0.0
        for point in points:
            day_duration = duration_by_track_day.get((track, point["date"]), 0.0)
            point["cumulativeHours"] = round(cumulative, 2)
            cumulative += day_duration / 3600.0

    # Add 7-day rolling averages
    for track in track_timeseries:
        _add_rolling_averages(track_timeseries[track])

    # ── Weekly and monthly aggregations (enhanced) ─────────────────
    lap_times_by_track_week: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    lap_times_by_track_month: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    for (track, date_str), times in lap_times_by_track_day.items():
        week_key = _iso_week(date_str)
        month_key = date_str[:7]
        lap_times_by_track_week[(track, week_key)].extend(times)
        lap_times_by_track_month[(track, month_key)].extend(times)

    def _build_agg_point(date_key: str, times: List[float]) -> Dict:
        sorted_times = sorted(times)
        p25 = _percentile(sorted_times, 0.25)
        p75 = _percentile(sorted_times, 0.75)
        return {
            "date": date_key,
            "bestLap": round(sorted_times[0], 3),
            "medianLap": round(median(sorted_times), 3),
            "stdDev": round(pstdev(sorted_times), 3) if len(sorted_times) > 1 else 0.0,
            "completeLaps": len(sorted_times),
            "worstLap": round(sorted_times[-1], 3),
            "p25": round(p25, 3),
            "p75": round(p75, 3),
            "iqr": round(p75 - p25, 3),
        }

    track_weekly: Dict[str, List[Dict]] = defaultdict(list)
    for (track, week_start), times in sorted(lap_times_by_track_week.items()):
        if times:
            track_weekly[track].append(_build_agg_point(week_start, times))

    track_monthly: Dict[str, List[Dict]] = defaultdict(list)
    for (track, month), times in sorted(lap_times_by_track_month.items()):
        if times:
            track_monthly[track].append(_build_agg_point(month, times))

    # ── Incident event trends ──────────────────────────────────────
    cur.execute("SELECT session_id, event_type FROM events")
    event_rows = cur.fetchall()
    incidents_by_track_day: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(
        lambda: {"off_track": 0, "spin": 0, "big_save": 0}
    )
    for session_id, event_type in event_rows:
        key = session_track_date.get(int(session_id))
        if not key:
            continue
        track, date_str = key
        if event_type in ("off_track", "spin", "big_save"):
            incidents_by_track_day[(track, date_str)][event_type] += 1

    # Count ALL laps (including resets) per track per day for incident rate
    # denominator — incidents happen during reset attempts too, so dividing
    # only by completed non-reset laps inflates the rate
    cur.execute("SELECT session_id FROM laps WHERE is_complete = 1")
    all_laps_by_track_day: Dict[Tuple[str, str], int] = defaultdict(int)
    for (session_id,) in cur.fetchall():
        key = session_track_date.get(int(session_id))
        if key:
            all_laps_by_track_day[key] += 1

    track_incidents: Dict[str, List[Dict]] = defaultdict(list)
    all_incident_dates = (
        set(incidents_by_track_day.keys())
        | set(all_laps_by_track_day.keys())
        | set(resets_by_track_day.keys())
    )
    for track, date_str in sorted(all_incident_dates):
        counts = incidents_by_track_day.get(
            (track, date_str), {"off_track": 0, "spin": 0, "big_save": 0}
        )
        resets = resets_by_track_day.get((track, date_str), 0)
        completed_laps = all_laps_by_track_day.get((track, date_str), 0)
        total_events = counts["off_track"] + counts["spin"] + counts["big_save"] + resets
        attempts = completed_laps + resets
        if attempts == 0 and total_events == 0:
            continue
        events_per_lap = round(total_events / attempts, 3) if attempts > 0 else 0.0
        resets_per_lap = round(resets / attempts, 3) if attempts > 0 else 0.0
        track_incidents[track].append({
            "date": date_str,
            "offTracks": counts["off_track"],
            "spins": counts["spin"],
            "bigSaves": counts["big_save"],
            "resets": resets,
            "resetsPerLap": resets_per_lap,
            "eventsPerLap": events_per_lap,
            "totalLaps": attempts,
        })

    # ── Session type distribution (by ISO week) ────────────────────
    track_session_types: Dict[str, List[Dict]] = defaultdict(list)
    if has_classified:
        types_by_track_week: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(
            lambda: {"corner_isolation": 0, "hot_laps": 0, "race_sim": 0, "mixed": 0}
        )
        for sid, stype in session_classified_type.items():
            key = session_track_date.get(sid)
            if not key or not stype:
                continue
            track, date_str = key
            week = _iso_week(date_str)
            if stype in types_by_track_week[(track, week)]:
                types_by_track_week[(track, week)][stype] += 1

        for (track, week_start), counts in sorted(types_by_track_week.items()):
            track_session_types[track].append({
                "date": week_start,
                "cornerIsolation": counts["corner_isolation"],
                "hotLaps": counts["hot_laps"],
                "raceSim": counts["race_sim"],
                "mixed": counts["mixed"],
            })

    # ── Sector times ───────────────────────────────────────────────
    track_sectors: Dict[str, List[Dict]] = defaultdict(list)
    if has_sector_table:
        sector_query = (
            """
            SELECT st.session_id, st.sector_name, st.sector_time
            FROM sector_times st
            JOIN laps l ON st.lap_id = l.id
            WHERE l.is_clean = 1
            """
            if has_clean
            else """
            SELECT st.session_id, st.sector_name, st.sector_time
            FROM sector_times st
            JOIN laps l ON st.lap_id = l.id
            WHERE l.is_complete = 1 AND l.is_reset = 0
            """
        )
        try:
            cur.execute(sector_query)
            sector_rows = cur.fetchall()
            sector_times_grouped: Dict[Tuple[str, str, str], List[float]] = defaultdict(list)
            for s_session_id, sector_name, sector_time in sector_rows:
                key = session_track_date.get(int(s_session_id))
                if not key or sector_time is None:
                    continue
                track, date_str = key
                sector_times_grouped[(track, date_str, sector_name)].append(float(sector_time))

            sector_by_track_date: Dict[Tuple[str, str], Dict[str, Dict]] = defaultdict(dict)
            for (track, date_str, sector_name), stimes in sector_times_grouped.items():
                sorted_st = sorted(stimes)
                sector_by_track_date[(track, date_str)][sector_name] = {
                    "best": round(sorted_st[0], 3),
                    "median": round(median(sorted_st), 3),
                    "stddev": round(pstdev(sorted_st), 3) if len(sorted_st) > 1 else 0.0,
                }

            for (track, date_str), sectors in sorted(sector_by_track_date.items()):
                track_sectors[track].append({"date": date_str, "sectors": sectors})
        except sqlite3.OperationalError:
            pass

    # ── Gap-to-reference ───────────────────────────────────────────
    references: Dict = {}
    refs_path = output_root / "references.json"
    if refs_path.exists():
        references = json.loads(refs_path.read_text(encoding="utf-8"))

    track_gap: Dict[str, List[Dict]] = defaultdict(list)
    for track, series in track_timeseries.items():
        safe_name = track.replace(" ", "-")
        ref = references.get(safe_name, {})
        alien_best = ref.get("alienBest")
        top_split = ref.get("topSplit")
        if alien_best is None and top_split is None:
            continue
        for point in series:
            entry: Dict = {"date": point["date"], "bestLap": point["bestLap"]}
            if alien_best is not None:
                entry["gapToAlien"] = round(point["bestLap"] - alien_best, 3)
            if top_split is not None:
                entry["gapToTopSplit"] = round(point["bestLap"] - top_split, 3)
            track_gap[track].append(entry)

    # ── Zone definitions from track configs ────────────────────────
    track_zones: Dict[str, List[Dict]] = {}
    if tracks_config_dir.exists():
        for config_file in sorted(tracks_config_dir.glob("*.json")):
            try:
                config = json.loads(config_file.read_text(encoding="utf-8"))
                track_id = config.get("track_id", config_file.stem)
                zones = config.get("zones", [])
                safe_name = track_id.replace(" ", "-")
                track_zones[safe_name] = zones
            except (json.JSONDecodeError, KeyError):
                continue

    # ── Baseline session exports ───────────────────────────────────
    baselines_by_track: Dict[str, List[Dict]] = defaultdict(list)
    if has_baseline:
        cur.execute(
            """
            SELECT id, file_path, session_start_time, session_end_time
            FROM sessions WHERE is_baseline = 1
            """
        )
        baseline_sessions = cur.fetchall()
        for bsid, bpath, bstart, bend in baseline_sessions:
            bkey = session_track_date.get(int(bsid))
            if not bkey:
                continue
            btrack, bdate = bkey

            if has_clean:
                cur.execute(
                    "SELECT lap_time FROM laps WHERE session_id = ? AND is_clean = 1 ORDER BY lap_number",
                    (bsid,),
                )
            else:
                cur.execute(
                    "SELECT lap_time FROM laps WHERE session_id = ? AND is_complete = 1 AND is_reset = 0 ORDER BY lap_number",
                    (bsid,),
                )
            lap_times = [float(r[0]) for r in cur.fetchall() if r[0] is not None and r[0] > 0]
            if not lap_times:
                continue

            sorted_laps = sorted(lap_times)
            p25 = _percentile(sorted_laps, 0.25)
            p75 = _percentile(sorted_laps, 0.75)

            baseline_entry: Dict = {
                "date": bdate,
                "sessionId": int(bsid),
                "laps": [round(t, 3) for t in lap_times],
                "median": round(median(sorted_laps), 3),
                "best": round(sorted_laps[0], 3),
                "worst": round(sorted_laps[-1], 3),
                "stddev": round(pstdev(sorted_laps), 3) if len(sorted_laps) > 1 else 0.0,
                "iqr": round(p75 - p25, 3),
                "p25": round(p25, 3),
                "p75": round(p75, 3),
                "cleanLapCount": len(lap_times),
            }

            if has_sector_table:
                try:
                    sq = (
                        """
                        SELECT st.sector_name, st.sector_time
                        FROM sector_times st JOIN laps l ON st.lap_id = l.id
                        WHERE st.session_id = ? AND l.is_clean = 1
                        """
                        if has_clean
                        else """
                        SELECT st.sector_name, st.sector_time
                        FROM sector_times st JOIN laps l ON st.lap_id = l.id
                        WHERE st.session_id = ? AND l.is_complete = 1 AND l.is_reset = 0
                        """
                    )
                    cur.execute(sq, (bsid,))
                    sec_rows = cur.fetchall()
                    sec_data: Dict[str, List[float]] = defaultdict(list)
                    for sname, stime in sec_rows:
                        if stime is not None:
                            sec_data[sname].append(float(stime))
                    if sec_data:
                        sectors: Dict[str, Dict] = {}
                        for sname, stimes in sec_data.items():
                            ss = sorted(stimes)
                            sectors[sname] = {
                                "best": round(ss[0], 3),
                                "median": round(median(ss), 3),
                            }
                        baseline_entry["sectors"] = sectors
                except sqlite3.OperationalError:
                    pass

            baselines_by_track[btrack].append(baseline_entry)

    # ── Write outputs ──────────────────────────────────────────────
    latest_day = max(daily_data.keys()) if daily_data else None

    monthly_breakdown = []
    for month_key in sorted(monthly_accum.keys()):
        acc = monthly_accum[month_key]
        monthly_breakdown.append({
            "month": month_key,
            "sessions": int(acc["sessions"]),
            "hours": round(acc["duration_s"] / 3600, 2),
            "laps": int(acc["laps"]),
        })

    overall_bins = build_bins(valid_reset_values)

    summary = {
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "totalSessions": total_sessions,
        "totalLaps": total_laps,
        "totalHours": round(total_duration_s / 3600, 2),
        "totalResets": total_resets,
        "resetHotspotsBins": overall_bins,
        "latestDay": latest_day,
        "monthlyBreakdown": monthly_breakdown,
        # Backward compat
        "jan2026Sessions": int(monthly_accum.get("2026-01", {}).get("sessions", 0)),
        "jan2026Hours": round(monthly_accum.get("2026-01", {}).get("duration_s", 0) / 3600, 2),
        "feb2026Sessions": int(monthly_accum.get("2026-02", {}).get("sessions", 0)),
        "feb2026Hours": round(monthly_accum.get("2026-02", {}).get("duration_s", 0) / 3600, 2),
    }

    (output_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # Clean stale output files before writing
    for subdir in ("daily", "tracks", "baselines"):
        d = output_root / subdir
        if d.exists():
            for old_file in d.glob("*.json"):
                old_file.unlink()

    # Daily files
    daily_dir = output_root / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    for date_str, day in daily_data.items():
        day_out = dict(day)
        day_out["durationHours"] = round(day_out.pop("durationSeconds") / 3600, 2)
        for tn, te in day_out["tracks"].items():
            te["durationHours"] = round(te.pop("durationSeconds") / 3600, 2)
        (daily_dir / f"{date_str}.json").write_text(json.dumps(day_out, indent=2), encoding="utf-8")

    # Track files + all new exports
    tracks_dir = output_root / "tracks"
    tracks_dir.mkdir(parents=True, exist_ok=True)
    for track_name, te in track_data.items():
        te["durationHours"] = round(te.pop("durationSeconds") / 3600, 2)
        safe_name = track_name.replace(" ", "-")

        (tracks_dir / f"{safe_name}.json").write_text(json.dumps(te, indent=2), encoding="utf-8")

        series = track_timeseries.get(track_name)
        if series:
            (tracks_dir / f"{safe_name}-timeseries.json").write_text(
                json.dumps(series, indent=2), encoding="utf-8"
            )
        weekly = track_weekly.get(track_name)
        if weekly:
            (tracks_dir / f"{safe_name}-weekly.json").write_text(
                json.dumps(weekly, indent=2), encoding="utf-8"
            )
        monthly = track_monthly.get(track_name)
        if monthly:
            (tracks_dir / f"{safe_name}-monthly.json").write_text(
                json.dumps(monthly, indent=2), encoding="utf-8"
            )
        incidents = track_incidents.get(track_name)
        if incidents:
            (tracks_dir / f"{safe_name}-incidents.json").write_text(
                json.dumps(incidents, indent=2), encoding="utf-8"
            )
        stypes = track_session_types.get(track_name)
        if stypes:
            (tracks_dir / f"{safe_name}-session-types.json").write_text(
                json.dumps(stypes, indent=2), encoding="utf-8"
            )
        sectors = track_sectors.get(track_name)
        if sectors:
            (tracks_dir / f"{safe_name}-sectors.json").write_text(
                json.dumps(sectors, indent=2), encoding="utf-8"
            )
        gap = track_gap.get(track_name)
        if gap:
            (tracks_dir / f"{safe_name}-gap.json").write_text(
                json.dumps(gap, indent=2), encoding="utf-8"
            )
        zones = track_zones.get(safe_name)
        if zones:
            (tracks_dir / f"{safe_name}-zones.json").write_text(
                json.dumps(zones, indent=2), encoding="utf-8"
            )

    # Baselines
    baselines_dir = output_root / "baselines"
    baselines_dir.mkdir(parents=True, exist_ok=True)
    for track_name, entries in baselines_by_track.items():
        safe_name = track_name.replace(" ", "-")
        (baselines_dir / f"{safe_name}.json").write_text(
            json.dumps(entries, indent=2), encoding="utf-8"
        )

    conn.close()


if __name__ == "__main__":
    main()
