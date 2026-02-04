# Spec Sheet: Track Config & Zone Tagging

## Purpose

Provide track-specific zones (sectors or corners) to map error events and hotspots.

## Storage

- JSON files stored in `tracks/`
- Filename matches `TrackName` from session info (e.g. `monza full.json`)

## Format

```json
{
  "track_id": "monza full",
  "zones": [
    {"name": "Sector 1", "start": 0.0, "end": 0.3333},
    {"name": "Sector 2", "start": 0.3333, "end": 0.6666},
    {"name": "Sector 3", "start": 0.6666, "end": 1.0}
  ]
}
```

## Notes

- `start` and `end` are in `LapDistPct` units (0.0 - 1.0)
- Zones can wrap the lap (start > end)
- Default fallback is no tagging if a config is missing
