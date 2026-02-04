# Spec Sheet: Incident & Error Detection

## Purpose

Detect loss-of-control events (spins, big saves) and off-track moments from telemetry.

## Inputs

- `SessionTime`
- `Lap`
- `Speed`
- `YawRate`
- `SteeringWheelAngle`
- `IsOnTrack`

## Outputs

- Event list with type, time, and lap number
- Summary counts by event type
- Hotspot buckets by `LapDistPct`

## Event Types

- `off_track`
- `spin`
- `big_save`

## Detection Heuristics

- Off-track: `IsOnTrack == 0` sustained for ~0.5s
- Spin: high yaw-rate sustained with minimum speed
- Big save: high yaw-rate + high steering input without spin

## Notes

Thresholds are conservative to reduce false positives and will be tuned per car/track.
