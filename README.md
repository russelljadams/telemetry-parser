# Sim Racing Performance Lab — Telemetry Core

This repository is dedicated solely to iRacing `.ibt` telemetry ingestion, analysis, and reporting.

## What This Repo Contains

- `telemetry_parser/` telemetry ingestion and analysis pipeline
- `specs/` system spec sheets for shared understanding
- `tracks/` track zone configurations for hotspot tagging
- `reports/` generated session reports
- `data/` SQLite database storage

## Experiment Charter

12-month skill acquisition protocol. iRacing. Super Formula Lights. 1,200+ hours under controlled constraints. Public documentation.

Parameters

- Duration: 12 months
- Target hours: 1,200+
- Car: SFL
- Tracks: Monza, Silverstone, Suzuka, Spa

Hypothesis

Skill acquisition follows predictable patterns when variables are constrained and feedback loops are tight. Most practice fails because it optimizes for volume over precision. This experiment tests whether deliberate practice methodology, applied with discipline, produces measurable elite-level improvement in a 12-month window.

Constraints

- One car. No switching. SFL chosen for clean feedback signal. Punishes slop, exposes fundamentals.
- Four tracks. Locked for 12 months. Depth over width.
- Warm tires only. Cold-tire laps excluded from measurement. Reproducibility is the baseline.
- High-quality hours only. Session without objective is not counted. Vibes laps are waste.

Advantage

Active reset. Instant reset to track. No recovery laps. Isolate a single corner, run it 50 times in the time real-world driving allows 10 laps. Rep density that physical practice cannot match. The feedback loop tightens to seconds.

Success Criteria

- Variance collapse. Standard deviation of lap times decreases over time.
- Retention. Near-pace after 24h+ away from sim.
- Transfer. Fundamentals hold on new track or car.
- Pressure stability. Race pace approximates practice pace.
- Articulation. Can explain why a lap was fast.

Status

Active. Experiment in progress. Awaiting first logged sessions.

## Purpose

Convert raw `.ibt` files into structured data, session metrics, incident detection, and actionable reports.
