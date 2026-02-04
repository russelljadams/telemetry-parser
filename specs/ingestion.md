# Spec Sheet: Ingestion Pipeline

## Purpose

Single command to ingest `.ibt` files into the database and create a session report.

## Inputs

- Path to `.ibt` file

## Outputs

- SQLite row in `sessions` and `laps`
- Markdown report per session

## Required Channels

- `SessionTime`
- `Lap`
- `LapDistPct`
- `LapLastLapTime`
- `LapCompleted`
- `PlayerIncidents`

## CLI

```
python -m telemetry_parser.ingest <file> --db data/telemetry.db --reports reports
```

## Edge Cases

- Missing channels
- Empty/partial files
