# Spec Sheet: Lap Segmentation

## Purpose

Segment telemetry into complete laps and Active Reset or partial attempts.

## Inputs

- `SessionTime`
- `Lap`
- `LapDistPct`
- `LapLastLapTime`
- `LapCompleted`

## Outputs

- `LapSegment` objects

## Logic

- Lap boundary detected when `LapDistPct` drops sharply or `Lap` increments
- A segment is complete when `LapCompleted` increases or lap increments
- Active Reset detection when `LapDistPct` drops without lap increment

## Metrics Derived

- Lap time per segment
- Complete vs partial attempts

## Edge Cases

- Zero or negative `LapLastLapTime`
- Session ending mid-lap
