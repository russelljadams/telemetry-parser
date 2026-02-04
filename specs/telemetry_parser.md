# Spec Sheet: Telemetry Parser

## Purpose

Parse iRacing `.ibt` telemetry files into structured, deterministic data that powers segmentation, metrics, and reports.

## Inputs

- `.ibt` telemetry file

## Outputs

- Parsed session metadata
- Structured channel data
- Record iterator for channel extraction

## Required Channels

- `SessionTime`
- `Lap`
- `LapDistPct`
- `LapLastLapTime`
- `LapCompleted`
- `PlayerIncidents`

## Responsibilities

- Read header, disk header, variable headers, and session info
- Provide channel lookup by name
- Iterate telemetry records with selected channels

## Non-Goals

- Real-time streaming
- Visualization
- Web UI

## Edge Cases

- Partial session files
- Missing channels
- Incomplete record buffers
