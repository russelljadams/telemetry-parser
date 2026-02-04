# Spec Sheet: Metrics

## Purpose

Compute session-level and lap-level performance metrics from segments.

## Inputs

- `LapSegment` list
- `PlayerIncidents`

## Outputs

- Lap time distribution stats
- Incident counts per lap

## Metrics

- Best, median, worst lap
- Standard deviation of lap times
- IQR of lap times
- Incident deltas per lap

## Edge Cases

- No complete laps
- Single complete lap
