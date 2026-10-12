# AISStream coverage probe: implementation plan

Status: proposed, not implemented. Written 2026-10-12.

## Context

Before any streaming infrastructure is justified, we need evidence that AISStream actually delivers usable AIS data for the Strait of Hormuz and Bab el-Mandeb. This plan covers one small Python probe that connects, records real messages for a configurable duration, and prints a coverage summary. No Kafka, Flink, PostgreSQL or dashboard.

## What I found

### Repository

- Only `README.md` is tracked. There is no Docker, Kafka or Compose file to avoid.
- An untracked `.venv` (Python 3.14.2) already holds `websockets 15.0.1`, `pytest 9.1.1`, `ruff` and `python-dotenv`.
- There is no root `.gitignore` and no dependency manifest.
- `AISSTREAM_API_KEY` is set (checked for presence only, never printed).

### AISStream API

`aisstream.io/documentation` returned HTTP 403 to my fetcher, so the points below come from AISStream's own GitHub repos (`aisstream/example`, `aisstream/ais-message-models`, `aisstream/issues`) plus one third-party AsyncAPI profile. Items marked *unverified* get confirmed against the first live messages.

- Endpoint `wss://stream.aisstream.io/v0/stream`. The official Python example enables deflate compression, saying it is required for full message bandwidth.
- Subscription: `{"APIKey", "BoundingBoxes", "FiltersShipMMSI"?, "FilterMessageTypes"?}`. Each box is two `[latitude, longitude]` corners. It must be sent within about 3 seconds of connecting (*unverified*, third-party source).
- Envelope: `MessageType`, `MetaData`, `Message[<MessageType>]`.
- `MetaData` has no official schema (the OpenAPI definition types it as a bare object). Reported keys are `MMSI`, `ShipName`, lowercase `latitude` / `longitude`, and `time_utc` in Go format such as `2022-12-29 18:22:32.318353 +0000 UTC`. One source shows capitalised coordinate keys, so the parser accepts both (*unverified*).
- `PositionReport` carries `UserID`, `Latitude`, `Longitude`, `Sog`, `Cog`, `TrueHeading`, `Valid` and `Timestamp`. `Timestamp` is only the UTC second of the minute (0-59, 60+ means unavailable). `LongRangeAisBroadcastMessage` has no `Timestamp` at all.
- There are 25 message types in the official enum. The four that carry positions are `PositionReport`, `StandardClassBPositionReport`, `ExtendedClassBPositionReport` and `LongRangeAisBroadcastMessage`.

### Reliability signals from the issue tracker

These shape the design.

- Aug 2026: an outage of about 11 days where connections stayed open and subscriptions were accepted but zero messages arrived (#257, #259). The maintainer blamed a circuit-breaker bug under rising load. A user reported a recurrence on 2026-08-20.
- Sep 2026: users report HTTP 429 at the handshake, apparently about 2 concurrent connections per account, and 100-140 server-side disconnects per day (#300, #302, #292). No maintainer reply.
- Coverage is station-dependent, with open gap reports for several regions (#251, #255, #202, #183). I found nothing either way for the Gulf or Red Sea.

## Architectural decisions

| Decision | Choice | Reason |
|---|---|---|
| Process model | One asyncio process, one WebSocket | Smallest thing that answers the question; respects the apparent 2-connection account limit |
| Runtime dependencies | `websockets` only | Already installed; everything else is stdlib |
| Liveness check | Small control box in the same subscription | Separates "feed is dead" from "region is empty" without a second connection |
| Message type filter | None by default | Counts every type; a `BaseStationReport` inside a region is direct evidence of a shore receiver |
| Coordinate source | Message body first, `MetaData` as fallback | The body is the AIS payload; `MetaData` has no schema |
| Timestamps | Three clocks kept separate, event time derived | `time_utc` is probably a receive-side stamp and `Timestamp` has no date or minute |
| Storage | Append-only JSONL on local disk | Raw evidence, replayable later, no infrastructure |
| Processing | Inline in the receive loop | No application queue that could grow without bound |
| Reconnect | Exponential backoff with jitter, capped attempts | Avoids locking the account out with 429s |
| Package layout | Top-level `aisprobe/` package | Runs with `python -m aisprobe` from the repo root, no install step needed |

## File structure

```
.gitignore              data/, .venv/, __pycache__/, .pytest_cache/, .ruff_cache/, .env
pyproject.toml          deps: websockets (runtime), pytest + ruff (dev); pytest and ruff config
README.md               short "coverage probe" section: setup and run
docs/coverage-probe.md  assumptions, limitations, how to read the summary
aisprobe/
  __init__.py
  __main__.py           python -m aisprobe
  regions.py            Region dataclass, box definitions, classify(lat, lon)
  parsing.py            envelope -> Observation or a named reject reason; time_utc parser
  stats.py              ProbeStats: counters, per-minute buckets, gaps, lag histograms, summary
  probe.py              connect / subscribe / receive / reconnect loop, raw writer, argparse CLI
tests/
  test_regions.py
  test_parsing.py
  test_stats.py
data/                   gitignored: raw/probe-<UTC>.jsonl, summaries/probe-<UTC>.json
```

## Dependencies

- Runtime: `websockets` (15.0.1 is installed).
- Dev: `pytest`, `ruff`.
- Stdlib for the rest: `asyncio`, `json`, `argparse`, `datetime`, `collections`.
- `python-dotenv` is installed in the venv but will not be used.
- The existing `.venv` is reused, with `pip install -e ".[dev]"` to align it with `pyproject.toml`.

## Implementation approach

### Regions

Configurable, `[lat, lon]` corners, approaches included on purpose.

| Region | Latitude | Longitude | Covers |
|---|---|---|---|
| `hormuz` | 24.5 to 27.5 | 54.5 to 58.0 | Strait, Bandar Abbas, Fujairah anchorage, Jebel Ali approach |
| `bab_el_mandeb` | 11.0 to 14.5 | 41.5 to 45.5 | Strait, Djibouti, Aden approach, southern Red Sea |
| `control` | 51.8 to 52.1 | 3.8 to 4.6 | Rotterdam approaches (liveness check only) |

Control traffic is tagged and reported separately and never counts toward regional figures. `--no-control` turns it off. A message whose coordinates fall in no box is counted as `unclassified`.

### Connection

- One WebSocket, subscription sent immediately after the handshake, deflate compression on.
- Reconnect uses exponential backoff with jitter (2 s up to 60 s) and a capped attempt count.
- Auth errors and repeated 429s stop the run with a diagnosis and no retry loop.
- A watchdog records a stall after 120 s of total silence and reconnects once per stall.
- The key is read from the environment, sent only in the subscription frame, and never logged. Exception text is scrubbed before printing.

### Raw capture

- One JSONL line per frame, written as it arrives: `{"ingest_ts", "conn", "region", "msg"}`, or `"raw_text"` when the frame is not valid JSON.
- `ingest_ts` is UTC ISO-8601 with microseconds.
- A `--max-raw-mb` cap stops raw writing (not counting) if hit.

### Parsing

- Extracted fields: MMSI, latitude, longitude, speed (SOG), course (COG), heading, `ais_second`, `source_time`.
- Coordinates come from the message body, falling back to `MetaData`. Disagreements between the two are counted.
- AIS "not available" sentinels (lat 91, lon 181, SOG 102.3, COG 360, heading 511, timestamp 60+) become `None`.
- Non-JSON frames, missing keys, wrong types, error frames and unknown message types each increment a named reject counter and never raise.

### Three clocks

- `ais_second`: `Timestamp` from the transponder (second of minute only).
- `source_time`: parsed `MetaData.time_utc`.
- `ingest_time`: our receipt time.
- `event_time` is derived by snapping `source_time` to the nearest minute consistent with `ais_second`.

The summary reports the share of observations with usable `ais_second`, the `source_time - event_time` and `ingest_time - source_time` distributions (p50 / p95 / max), and the per-vessel out-of-order rate. Those numbers answer whether event-time processing is viable and how much watermark lateness it would need.

### Stats and bounded memory

- Per region: message count, position count, unique MMSIs, per-minute buckets, inter-arrival gaps over `--gap-threshold` (default 60 s) with count and longest, and empty minutes.
- Unique-MMSI sets are capped at 50k with an overflow flag.
- Per-minute buckets are bounded by the probe duration.
- Lags go into fixed-bucket histograms.
- Per-vessel last event time lives in a capped LRU dict.
- Connection log: connects, close codes, downtime seconds, stalls.

### Summary

Printed as text and saved as JSON: per-region counts, unique vessels, mean and peak msgs/min, gaps, timestamp findings, connection interruptions, and a plain diagnosis line.

With zero regional data the diagnosis names one of:

- auth rejected
- throttled (429)
- upstream silent (control also empty)
- no regional coverage (control flowing)
- subscription misconfigured

### CLI

```
python -m aisprobe --duration 900 [--regions ...] [--no-control] \
    [--gap-threshold 60] [--out data] [--max-raw-mb 200]
```

Ctrl-C still writes the summary.

## Implementation steps

1. Add `.gitignore` and `pyproject.toml`; align the venv with `pip install -e ".[dev]"`.
2. `regions.py` and `test_regions.py`.
3. `parsing.py` and `test_parsing.py`.
4. `stats.py` and `test_stats.py`.
5. `probe.py` and `__main__.py`: connection loop, raw writer, CLI.
6. 60-second live smoke run; correct the parser against real frames where the *unverified* points differ.
7. 15-minute probe; review the summary against the success criteria.
8. Write `docs/coverage-probe.md` and the README section, including the observed results.
9. Commit locally on a branch after review. No push without approval.

## Testing requirements

Hand-written message fixtures are used for unit tests only. They are never written to `data/` or counted in any summary. After the first live run, a few captured real frames replace them.

- `test_regions.py`: points inside, outside and on the edge of each box; box sanity (lat within ±90, min below max) to catch swapped lat/lon; no overlap between regions.
- `test_parsing.py`: each position type; `MetaData` in both casings; body/MetaData fallback; sentinels to `None`; `time_utc` with 6- and 9-digit fractions; malformed JSON, missing `Message`, error frame, unknown type.
- `test_stats.py`: per-minute bucketing, gap detection, unique-vessel cap, event-time derivation across a minute boundary.

## Verification

1. `.venv/bin/python -m pytest` and `.venv/bin/ruff check .`
2. 60-second live smoke run. Inspect a few raw lines to confirm `MetaData` casing and `time_utc` format.
3. 15-minute run: `.venv/bin/python -m aisprobe --duration 900`. Check the summary against each success criterion.
4. `git status` shows nothing under `data/`; grep the output and raw file to confirm the key is absent.

### Success criteria mapping

| Question | Where the summary answers it |
|---|---|
| Are both regions delivering data? | Per-region message and position counts, diagnosis line |
| Position observations received | Per-region position count |
| Unique vessels | Per-region unique MMSIs |
| Average and peak rates | Per-minute buckets: mean and max |
| Timestamps suitable for event time? | Usable `ais_second` share, lag distributions, out-of-order rate |
| Gap frequency | Gap count, longest gap, empty minutes |
| Reliable enough to continue? | Connection interruptions, stalls, control vs regional comparison |

## Open questions

1. **Do the two regions have any receiver coverage?** This is the main thing the probe exists to answer. A realistic outcome is sparse or zero data.
2. **What does `time_utc` actually represent?** Probably AISStream's receive time, not the transponder's clock. The probe measures this.
3. **`MetaData` key casing and exact `time_utc` format.** Sources disagree. Confirmed in the smoke run.
4. **Is the 3-second subscription deadline real?** Third-party claim only. The probe sends immediately, so it does not depend on the answer.
5. **Connection and rate limits.** Undocumented. User reports suggest about 2 concurrent connections per account. Any other client using this key at the same time may cause 429s.
6. **Are the box extents right?** They include port approaches, which helps detect coverage but inflates counts relative to strait transits. Tighter corridor boxes may be wanted later.
7. **Is the control box acceptable?** It adds unrelated Rotterdam traffic to the raw file (tagged). The alternative is `--no-control` and a weaker zero-data diagnosis.
8. **Is one 15-minute sample enough?** Coverage varies by time of day and station uptime. I recommend repeating at 2-3 different hours before a go/no-go.
9. **Fallback data source.** If coverage is absent, the project needs a different feed or different regions. Not scoped here.

## Limitations to document

- Terrestrial, volunteer-fed coverage. Absence of data is not absence of ships; AIS switch-off and spoofing are also common in both regions.
- `time_utc` is a receive-side stamp and `Timestamp` has no date or minute, so event time is reconstructed, not transmitted.
- Free service with no SLA, undocumented limits and a recent multi-day outage.
- Regional boxes include port approaches, so counts are not strait transits.
- The official documentation page could not be fetched during planning (HTTP 403), so schema details rest on AISStream's GitHub repos until the smoke run confirms them.

## Out of scope

Kafka, Flink, PostgreSQL, the dashboard, any simulated or synthetic traffic, and pushing commits.

## Sources

- [aisstream/example: Python client](https://raw.githubusercontent.com/aisstream/example/main/python/main.py)
- [aisstream/ais-message-models: type definitions](https://raw.githubusercontent.com/aisstream/ais-message-models/master/type-definition.yaml)
- [aisstream/issues](https://github.com/aisstream/issues/issues): [#257](https://github.com/aisstream/issues/issues/257), [#300](https://github.com/aisstream/issues/issues/300), [#251](https://github.com/aisstream/issues/issues/251)
- [Third-party AsyncAPI profile (api-evangelist)](https://raw.githubusercontent.com/api-evangelist/aisstream/refs/heads/main/asyncapi/aisstream-asyncapi.yml)
- [AISStream documentation](https://aisstream.io/documentation) (not retrievable during planning)
