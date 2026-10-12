# AISStream coverage probe

A small Python probe that records real AIS messages from AISStream for a fixed window and reports what arrived for the Strait of Hormuz and Bab el-Mandeb. It never generates or simulates traffic.

Design background and open questions are in [aisstream-implementation-plan.md](aisstream-implementation-plan.md).

## Running it

```bash
python -m venv .venv                       # once
.venv/bin/pip install -e ".[dev]"          # once
export AISSTREAM_API_KEY=...               # already set in the Codespace

.venv/bin/python -m pytest
.venv/bin/ruff check .
.venv/bin/python -m aisprobe --duration 60     # smoke test
.venv/bin/python -m aisprobe --duration 900    # 15-minute probe
```

| Option | Default | Meaning |
|---|---|---|
| `--duration` | 900 | Probe length in seconds |
| `--regions` | `hormuz,bab_el_mandeb` | Target regions to subscribe to |
| `--no-control` | off | Skip the Rotterdam control region |
| `--gap-threshold` | 60 | Seconds without a position that count as a gap |
| `--out` | `data` | Output directory (ignored by Git) |
| `--max-raw-mb` | 200 | Raw file size cap; statistics continue after it |
| `--stall-timeout` | 120 | Seconds of total silence that count as a stall |
| `--max-reconnects` | 8 | Consecutive failed connection attempts allowed |

Exit code is 0 for a completed run, 1 for an incomplete one, 2 when the API key is missing.

## Output

- `data/raw/probe-<UTC>.jsonl`: one line per frame, `{"ingest_ts", "conn", "region", "msg"}`. Frames that are not valid JSON are kept under `raw_text` instead of `msg`.
- `data/summaries/probe-<UTC>.json`: the full summary. A text version is printed.

## Regions

Corners are `[latitude, longitude]`.

| Region | Latitude | Longitude | Role |
|---|---|---|---|
| `hormuz` | 24.5 to 27.5 | 54.5 to 58.0 | Target |
| `bab_el_mandeb` | 11.0 to 14.5 | 41.5 to 45.5 | Target |
| `control` | 51.8 to 52.1 | 3.8 to 4.6 | Rotterdam approaches, liveness only |

Control traffic is reported separately and never counts toward a target region.

## Reading the summary

The summary keeps three things apart, because none implies the next:

1. **WebSocket status**: whether the connection opened, the subscription was sent and confirmed, and whether it was interrupted.
2. **Control traffic**: whether the feed delivered anything at all.
3. **Target-region observations**: what arrived for each region.

When a target region has no data, the summary states what that is consistent with and does not conclude that coverage is absent.

An incomplete run (interrupted, throttled, auth rejected, reconnects exhausted) is flagged at the top of the text summary and in `run.completed` / `run.end_reason`. `raw_capture.cap_reached` records whether the raw file hit its size cap.

## Timestamps

| Name | Source | What it is |
|---|---|---|
| `ais_second` | `Message.<type>.Timestamp` | Transponder's UTC second of the minute. No minute, hour or date. 60 and above means unavailable. |
| `source_time` | `MetaData.time_utc` | AISStream's stamp. Undocumented; treated as receive-side. |
| `ingest_time` | this probe | Local clock when the frame was received. |

The probe also reports an **estimated** event time: `source_time` snapped to the nearest minute whose seconds equal `ais_second`. It is always within 30 seconds of `source_time` by construction, so it cannot show a delay longer than that. It is not verified event time and should not be used to choose watermark settings.

## Schema confirmed on the live feed (2026-10-12)

- All frames arrive as binary WebSocket frames containing UTF-8 JSON.
- Top-level keys: `MessageType`, `MetaData`, `Message`.
- `MetaData` keys: `MMSI` (integer), `MMSI_String` (also an integer), `ShipName`, lowercase `latitude` and `longitude`, `time_utc`.
- `time_utc` looks like `2026-10-12 18:15:04.057960188 +0000 UTC`, with nanosecond digits.
- Position bodies carry capitalised `Latitude` and `Longitude`, plus `UserID`, `Sog`, `Cog`, `TrueHeading`, `Timestamp`.
- The first frame after subscribing is an undocumented service frame with no `MetaData`: `{"MessageType": "SubscriptionConfirmation", "Message": {"CompressionEnabled": true}}`.

## Assumptions

- Coordinates in the message body are preferred; `MetaData` is the fallback and is the only source for non-position messages.
- A message belongs to the first region whose box contains its coordinates.
- Gaps are measured between position observations by ingest time, including the stretch before the first and after the last one.
- Unit-test fixtures are hand-written and never written to `data/` or counted in a summary.

## Known limitations

- Coverage depends on volunteer shore receivers. Absence of data is not absence of ships, and AIS switch-off and spoofing occur in both regions.
- Target boxes include port approaches, so counts are not strait transits.
- One window is a snapshot; coverage varies with time of day and receiver uptime.
- AISStream is a free service with no SLA and undocumented connection limits. The probe uses a single connection.
- Percentiles come from a 5,000-value reservoir per region and are approximate above that.
- Unique vessels are capped at 50,000 per region; the summary flags the cap if hit.
- The raw file stores each message re-serialised from parsed JSON, not the original bytes.
- The official documentation page could not be fetched during planning, so the schema rests on AISStream's GitHub repos and the live observations above.
