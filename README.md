# Sim Racing Performance Lab — Telemetry Core

This repository is dedicated solely to iRacing `.ibt` telemetry ingestion, analysis, and reporting.

## What This Repo Contains

- `telemetry_parser/` telemetry ingestion and analysis pipeline
- `specs/` system spec sheets for shared understanding
- `tracks/` track zone configurations for hotspot tagging
- `reports/` generated session reports
- `data/` SQLite database storage

## Experiment Charter

12-month GT3 skill acquisition protocol. iRacing. Porsche 911 GT3 Cup. 1,200+ hours under controlled constraints. Public documentation.

Parameters

- Duration: 12 months (Feb 2026 – Feb 2027)
- Target hours: 1,200+ (100h/month, 90h floor)
- Car: Porsche 911 GT3 Cup
- Tracks: Spa, Monza, Nürburgring GP, Barcelona
- Scheduling: 3-phase increasing contextual interference (Porter & Magill, 2010)

Hypothesis

Skill acquisition follows predictable patterns when variables are constrained and feedback loops are tight. This experiment tests whether deliberate practice methodology — structured using motor learning research — produces measurable elite-level improvement in a 12-month window.

Why GT3

Formula sim racing has no viable real-world career path. GT3 does. Porsche Carrera Cup is the direct feeder to factory GT programs. Team RedLine bridges sim-to-real. All four tracks appear on actual Porsche Supercup or GTWC Europe calendars.

Prologue: SFL Phase (Jan 2 – Feb 12, 2026)

The experiment originally launched with Super Formula Lights. 113 hours were logged across 311 sessions before pivoting to GT3. That data is archived in the database and in `public/data/archive/sfl-phase/`.

Constraints

- One car. No switching. Porsche 911 GT3 Cup chosen for real-world career path alignment.
- Four tracks. Locked for 12 months. Depth over width.
- 3-phase scheduling. Blocked → serial rotation → full interleaving (research-backed).
- Warm tires only. Cold-tire laps excluded from measurement.
- High-quality hours only. Session without objective is not counted.

Success Criteria

- Variance collapse. Standard deviation of lap times decreases over time.
- Retention. Near-pace after 24h+ away from sim.
- Transfer. Fundamentals hold on new track or car.
- Pressure stability. Race pace approximates practice pace.
- Articulation. Can explain why a lap was fast.
- Competitive in Porsche Esports Supercup-level lobbies.

Status

Active. GT3 phase starting Feb 13, 2026.

## Purpose

Convert raw `.ibt` files into structured data, session metrics, incident detection, and actionable reports.

## Daily Ingest

Process each Porsche 911 GT3 Cup `.ibt` file per day, generate session reports/summaries, and write a daily rollup.

```
python3 scripts/daily_ingest.py \
  --source /media/sf_iracing \
  --start-date 2026-02-13 \
  --db data/telemetry.db \
  --reports reports \
  --summaries summaries \
  --daily-reports reports/daily

python3 scripts/build_site_data.py \
  --db data/telemetry.db \
  --output ../sim_racing_experiment/public/data
```

Backfill reset locations for existing sessions (optional):
```
python3 scripts/backfill_reset_events.py --db data/telemetry.db --start-date 2026-02-13
```

Notes:
- The car filename pattern is `porsche911*_<track> YYYY-MM-DD HH-MM-SS.ibt` (adjust regex in daily_ingest.py once first real IBT file confirms the exact car ID).
- Legacy SFL pattern (`superformulalights324_...`) is still supported for archived data.
- Output is directed to `../sim_racing_experiment/public/data` for the dashboard.
