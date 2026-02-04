# Spec Sheet: Storage

## Purpose

Persist session and lap metrics for longitudinal analysis.

## Storage Layer

- SQLite database

## Tables

- `sessions`
- `laps`

## Required Fields

Sessions

- File path
- Session time fields
- Record count
- Aggregated lap metrics

Laps

- Lap number
- Start/end time
- Lap time
- Flags for complete/reset
- Incidents

## Non-Goals

- Cloud storage
- Multi-user concurrency
