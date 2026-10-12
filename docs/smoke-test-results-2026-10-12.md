# AISStream coverage probe: 60-second smoke test results (2026-10-12)

The probe is built and the 60-second live smoke test ran cleanly: Hormuz delivered data, Bab el-Mandeb delivered nothing, and the Rotterdam control flowed normally. The 15-minute probe has not been started, and nothing is committed or pushed.

## Resume state

None of the six files from the interrupted session had been written, so they were rewritten unchanged and the work continued from there. The work is on a local branch, `aisstream-coverage-probe`.

## Files created

- `.gitignore`, `pyproject.toml`
- `aisprobe/`: `__init__.py`, `__main__.py`, `regions.py`, `parsing.py`, `stats.py`, `probe.py`
- `tests/`: `test_regions.py`, `test_parsing.py`, `test_stats.py`
- `docs/coverage-probe.md`, plus a short section appended to `README.md`
- Generated and ignored by Git: `data/raw/probe-20261012T181505Z.jsonl`, `data/summaries/probe-20261012T181505Z.json`

## Tests and lint

- **Tests:** 74 passed.
- **Lint:** `ruff check` and `ruff format --check` are clean.
- **Not run:** `pip install -e ".[dev]"`. The existing `.venv` already had `websockets`, `pytest` and `ruff`, so it was left alone.

## Connection and subscription

- One connection opened on the first attempt, with no interruptions, stalls or error frames over 60.7 s.
- The server confirmed the subscription.
- All 332 frames parsed and none were rejected.
- The API key does not appear in the raw file or the summary.

## Messages per region (60 s)

| Region | Messages | Positions | Unique vessels | Longest position gap |
|---|---|---|---|---|
| `hormuz` | 17 | 10 | 17 | 22.9 s |
| `bab_el_mandeb` | 0 | 0 | 0 | whole run |
| `control` (Rotterdam) | 314 | 255 | 297 | 1.4 s |

- **Hormuz is mostly Dubai:** 15 of the 17 messages sit around Dubai and Jebel Ali (about 25.0–25.3 N, 55.0–55.3 E) and are near-stationary. One vessel was under way at 14 knots at 26.15 N, 55.07 E. Nothing was seen in the strait itself (around 56–57 E).
- **Bab el-Mandeb is inconclusive:** control traffic arrived on the same connection, so the feed was delivering. Zero messages in 60 seconds fits no receiver coverage, but also a receiver outage or a quiet minute.

## Timestamp findings

- **Local receipt lag:** local receipt time trails AISStream's `time_utc` by a steady 2.3 s (p95 2.9 s) in both regions. That is pipeline delay plus any clock difference.
- **Transponder second:** present on 264 of 265 positions.
- **Estimated event time:** AISStream's stamp runs later than the estimate by a median of 0.9 s for Rotterdam and 7.6 s for Hormuz (range 3.5 to 12 s, 10 samples). That suggests `time_utc` is a receive-side stamp and that Hormuz data takes a slower path. The estimate cannot show delays over 30 s, so this is not a basis for watermark settings.
- **Ordering:** no out-of-order stamps, but only 7 vessels were seen twice, so this is barely tested.

## Schema findings

- Frames arrive as binary WebSocket frames holding JSON.
- `MetaData` keys are `MMSI`, `MMSI_String` (an integer despite the name), `ShipName`, lowercase `latitude` / `longitude`, and `time_utc`.
- `time_utc` carries nanoseconds, e.g. `2026-10-12 18:15:04.057960188 +0000 UTC`, and parsed every time.
- Body and `MetaData` coordinates agreed on every position message.

## Errors and unexpected behaviour

- **Undocumented frame, fixed:** the first frame is `{"MessageType": "SubscriptionConfirmation", ...}` with no `MetaData`. The probe had counted it as an unknown message; it is now a service frame reported as "subscription confirmed". The fix was verified by replaying the captured frames offline, not with a second live run.
- **About one message per vessel per minute:** in Rotterdam, 241 vessels appeared once and 7 twice. The feed may be downsampled per vessel, or 60 seconds is simply too short to tell. The 15-minute run should settle it.
- **Close code 1006:** when the probe closed the connection at the deadline, the server dropped it without a close handshake. It is harmless here and is not counted as an interruption.

## Open decision before the 15-minute run

The Hormuz box as drawn (24.5–27.5 N, 54.5–58.0 E) is largely measuring Dubai port traffic. Options: keep the box and add a breakdown by longitude to the summary, or tighten it to the strait, which may leave very little data. The recommendation is to keep it and add the breakdown.
