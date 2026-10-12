# Development checkpoint

Written 2026-10-12, end of session 1.

**Project:** Maritime Disruption Intelligence
**Repository:** `jimbyrnes/shipping-lanes`
**Branch:** `aisstream-coverage-probe` (local only, nothing committed or pushed)

## Where things stand

The AISStream coverage probe is built, tested, and has passed a 60-second live smoke test. The approved 15-minute coverage experiment has **not** been run. The next session starts there, after two small additions to the probe (subregions and per-vessel frequency statistics).

**All of today's work is uncommitted and exists only in this Codespace.** The raw capture under `data/` is ignored by Git and will never be in the repository. If the Codespace is deleted, both are lost.

## 1. Project objective

Build a maritime operations and geopolitical disruption intelligence platform that monitors commercial vessel activity in strategically important shipping corridors. Initial regions: the Strait of Hormuz and Bab el-Mandeb. It is a production-oriented streaming data engineering portfolio project.

### Intended architecture (not built)

```
AISStream WebSocket -> Python ingestion -> Apache Kafka -> Apache Flink -> PostgreSQL -> real-time dashboard
```

**Governing principle:** the business requirements must justify distributed streaming. Kafka and Flink are not to be introduced just to use them. Nothing beyond the Python probe exists yet, and none of Kafka, Flink, PostgreSQL or the dashboard should be started until the coverage question is answered.

## 2. Current milestone and scope

**Milestone:** AISStream coverage validation. Decide whether the feed has enough geographic coverage and per-vessel continuity in the two regions to justify further development.

**In scope:** one Python probe that records real AIS messages for a fixed window and reports coverage.

**Out of scope for this milestone:** Kafka, Flink, PostgreSQL, dashboard, any simulated or synthetic traffic.

## 3. Completed work

| Item | Status |
|---|---|
| Repository inspection and AISStream API research | Done |
| Implementation plan (`docs/aisstream-implementation-plan.md`) | Done, approved with three modifications |
| Probe package `aisprobe/` | Done |
| Unit tests (74) and lint configuration | Done, passing |
| 60-second live smoke test | Done |
| Schema validation against real frames | Done |
| Fix for the undocumented `SubscriptionConfirmation` frame | Done, verified by offline replay |
| User documentation (`docs/coverage-probe.md`, README section) | Done |
| 15-minute coverage probe | **Not run** |

The three approved modifications, all implemented:

1. **Event-time uncertainty.** Transponder, AISStream and local timestamps are kept separate. The reconstructed event time is labelled as estimated everywhere and is not used to prescribe watermark thresholds.
2. **Coverage diagnosis.** The summary reports WebSocket health, control traffic and regional observations as three separate facts, and states uncertainty when a target region has no data.
3. **Raw capture and interruptions.** The summary records whether the raw file hit its size cap, statistics continue past the cap, and incomplete runs are flagged.

## 4. Repository and branch state

```
Branch: aisstream-coverage-probe (created from main at a2c8dbc "Initial commit")
Commits on branch: none beyond main

 M README.md
?? .gitignore
?? aisprobe/        (__init__, __main__, parsing, probe, regions, stats)
?? docs/            (4 files, see below)
?? pyproject.toml
?? tests/           (test_parsing, test_regions, test_stats)
```

Ignore rules verified with `git check-ignore`:

| Path | Ignored by |
|---|---|
| `.env` (credentials) | `.gitignore` line 13 |
| `.venv/` | `.gitignore` line 5 |
| `data/` (raw AIS datasets and summaries) | `.gitignore` line 2 |
| `__pycache__/`, `.pytest_cache/`, `.ruff_cache/`, `*.egg-info/` | `.gitignore` |

The API key value does not appear in any file in the working tree, including the raw capture.

## 5. Dependencies and environment

- GitHub Codespaces, 2 vCPUs, about 8 GiB RAM, Docker available (unused so far).
- Python 3.14.2. Virtual environment at `.venv/` (pre-existing, reused).
- Installed in the venv: `websockets 15.0.1`, `pytest 9.1.1`, `ruff 0.17.0`. `python-dotenv 1.2.4` is installed but unused.
- `pyproject.toml` declares `websockets>=14` (runtime) and `pytest`, `ruff` (dev), `requires-python >=3.11`.
- `pip install -e ".[dev]"` has **not** been run. It is not needed: pytest is configured with `pythonpath = ["."]` and the probe runs from the repo root.
- `AISSTREAM_API_KEY` is supplied as an environment variable (Codespaces secret). It is read only by `aisprobe/probe.py`, sent only in the subscription frame, and scrubbed from all output.

## 6. File paths and commands

| Path | Purpose |
|---|---|
| `aisprobe/regions.py` | Region boxes and `classify(lat, lon)` |
| `aisprobe/parsing.py` | Frame parsing, `time_utc` parser, `estimate_event_time` |
| `aisprobe/stats.py` | Bounded statistics, summary builder, diagnosis, text formatter |
| `aisprobe/probe.py` | Connection loop, raw writer, CLI |
| `tests/` | 74 unit tests |
| `docs/aisstream-implementation-plan.md` | Approved plan, API research, open questions, sources |
| `docs/coverage-probe.md` | How to run the probe and read its output |
| `docs/smoke-test-results-2026-10-12.md` | Smoke test report as delivered |
| `docs/DEVELOPMENT_CHECKPOINT.md` | This file |
| `data/raw/probe-20261012T181505Z.jsonl` | Smoke test raw capture (332 lines, 211,185 bytes, Git-ignored) |
| `data/summaries/probe-20261012T181505Z.json` | Smoke test summary (Git-ignored) |

Run from the repository root:

```bash
.venv/bin/python -m pytest
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/python -m aisprobe --duration 60     # smoke test
.venv/bin/python -m aisprobe --duration 900    # 15-minute probe (NOT yet run)
.venv/bin/python -m aisprobe --help
```

Exit codes: 0 completed run, 1 incomplete run, 2 API key missing.

## 7. Smoke test findings (2026-10-12, 18:15:05 to 18:16:06 UTC)

### Checks

- 74 tests passed. `ruff check` and `ruff format --check` clean.
- One connection, opened on the first attempt. No interruptions, stalls or error frames.
- 332 frames received, none rejected. The server confirmed the subscription.

### Messages per region

| Region | Messages | Position reports | Unique vessels | Longest position gap |
|---|---|---|---|---|
| `hormuz` | 17 | 10 | 17 | 22.9 s |
| `bab_el_mandeb` | 0 | 0 | 0 | whole run |
| `control` (Rotterdam) | 314 | 255 | 297 | 1.4 s |

The remaining frame of the 332 is the subscription confirmation.

- **Hormuz is mostly Dubai.** 15 of the 17 messages were near Dubai and Jebel Ali (about 25.0–25.3 N, 55.0–55.3 E) and near-stationary. One vessel was under way at 14 knots at 26.15 N, 55.07 E. Nothing was observed inside the strait itself (about 56–57 E).
- **Bab el-Mandeb is inconclusive.** Control traffic arrived on the same connection, so the feed was delivering. Zero messages in 60 seconds is consistent with no receiver coverage, a receiver outage, or a quiet minute.

### Schema, confirmed on real frames

- All frames are binary WebSocket frames containing UTF-8 JSON.
- Top-level keys: `MessageType`, `MetaData`, `Message`.
- `MetaData` keys: `MMSI` (integer), `MMSI_String` (also an integer), `ShipName`, lowercase `latitude` / `longitude`, `time_utc`.
- `time_utc` format: `2026-10-12 18:15:04.057960188 +0000 UTC` (nanosecond digits). Parsed on every frame.
- Position bodies use capitalised `Latitude` / `Longitude` and agreed with `MetaData` on every position message.
- Message types seen: `PositionReport`, `StandardClassBPositionReport`, `ShipStaticData`, `StaticDataReport`.
- Undocumented first frame: `{"MessageType": "SubscriptionConfirmation", "Message": {"CompressionEnabled": true}}`, with no `MetaData`.

### Timestamps

- Local receipt trails `time_utc` by a steady 2.3 s (p95 2.9 s) in both regions.
- The transponder second (`Timestamp`) was present on 264 of 265 positions.
- `time_utc` runs later than the estimated event time by a median of 0.9 s for Rotterdam and 7.6 s for Hormuz (range 3.5 to 12 s, only 10 samples). This suggests `time_utc` is a receive-side stamp and that Hormuz data takes a slower path. It is an estimate that cannot show delays over 30 s.
- No out-of-order stamps, but only 7 vessels were seen twice, so ordering is barely tested.

## 8. Outstanding issues

1. **The `hormuz` box measures Dubai port traffic.** Subregions are needed before the long run (next step 1).
2. **Roughly one message per vessel per minute.** In Rotterdam, 241 vessels appeared once and 7 twice in 60 seconds. The feed may be downsampled per vessel, or the window was too short. This decides whether vessel trajectories are usable, so per-vessel frequency statistics are needed (next step 2).
3. **Bab el-Mandeb coverage is unknown.**
4. **The saved smoke-test summary JSON predates the `SubscriptionConfirmation` fix.** It shows that frame as one `unclassified` message with an unknown type. The code is fixed; the file was left as recorded. The figures in this document are the corrected ones.
5. **Close code 1006 at shutdown.** The server drops the connection without a close handshake when the probe closes at the deadline. Harmless, not counted as an interruption.
6. **Official AISStream documentation was never read.** `aisstream.io/documentation` returned HTTP 403 to the fetch tool. Schema knowledge rests on AISStream's GitHub repos and the live frames above.
7. **Feed reliability risk.** The AISStream issue tracker shows an outage of about 11 days in August 2026 (connections open, no data), and user reports in September of HTTP 429 at about 2 concurrent connections per account plus frequent disconnects. None of this was seen in the smoke test.
8. **Nothing is committed.** See the warning at the top.

## 9. Architectural decisions

| Decision | Choice | Reasoning |
|---|---|---|
| Validate before building | Probe first, no Kafka/Flink | The streaming stack is only justified if the feed has the coverage and volume to need it |
| Process model | One asyncio process, one WebSocket | Smallest thing that answers the question; stays under the apparent 2-connection account limit |
| Runtime dependencies | `websockets` only | Already installed; the rest is standard library |
| Control region | Rotterdam box in the same subscription | Separates "feed is silent" from "region is empty" without a second connection |
| Message type filter | None | Counts every type, so static data and any base-station reports are visible |
| Coordinate source | Message body, then `MetaData` | The body is the AIS payload; `MetaData` has no official schema |
| Timestamps | Three clocks kept separate; event time only estimated | `time_utc` appears to be receive-side and `Timestamp` has no minute or date |
| Diagnosis wording | Report observations, not verdicts | Missing data has several possible causes that one run cannot separate |
| Raw storage | Append-only JSONL with a size cap | Replayable evidence with no infrastructure; statistics continue past the cap |
| Memory | Capped sets and dicts, fixed-size reservoirs, no application queue | Usage must not grow with message volume |
| Reconnect | Exponential backoff with jitter, capped attempts, stop on auth failure or repeated 429 | Avoids locking the account out |
| Service frames | `SubscriptionConfirmation` handled as a control frame | It is not AIS traffic and gives a positive signal that the subscription was accepted |
| Test fixtures | Hand-written, never written to `data/` | No simulated observations may reach a dataset or summary |

## 10. Next steps, in priority order

The next session begins with the approved 15-minute coverage experiment. Before running it:

1. **Add geographic subregions** inside the Hormuz area to distinguish:
   - Persian Gulf approaches (Dubai, Jebel Ali and the western side)
   - the Strait of Hormuz proper
   - Gulf of Oman approaches (Fujairah and the eastern side)

   Exact boundaries are still to be chosen. Report counts, unique vessels and gaps per subregion.
2. **Add per-vessel observation-frequency statistics** to evaluate trajectory continuity: observations per vessel, the distribution of intervals between consecutive positions for the same vessel, and the share of vessels seen only once. Keep memory bounded.
3. **Preserve the Rotterdam control and all existing diagnostic metrics.**
4. **Run unit tests and lint checks.**
5. **Execute the 15-minute probe:** `.venv/bin/python -m aisprobe --duration 900`.
6. **Evaluate** whether AISStream offers enough geographic coverage and vessel continuity to justify proceeding. One window is a snapshot, so consider repeating at two or three different hours before a go/no-go.

Standing constraints: no simulated traffic, no Kafka/Flink/PostgreSQL/dashboard yet, no commit or push without approval.

## 11. Future ideas (not current requirements)

Recorded so they are not lost. None of these is planned or approved work.

- Verified vessel entry/exit and chokepoint transit detection
- Passage completion or "porousness" metrics
- Historical traffic baselines using sources such as IMF PortWatch
- Geopolitical disruption monitoring and correlation

## 12. Prompt to resume

```
Resume the Maritime Disruption Intelligence project on branch aisstream-coverage-probe.

Read docs/DEVELOPMENT_CHECKPOINT.md first and confirm the repository state matches it
(git status, tests, lint). Do not repeat the investigation it already records.

Then carry out section 10 "Next steps" in order:
1. Propose Hormuz subregion boundaries (Persian Gulf approaches, the strait proper,
   Gulf of Oman approaches) and wait for my approval of the boundaries.
2. Implement the subregions and the per-vessel observation-frequency statistics,
   keeping the Rotterdam control and all existing metrics.
3. Run unit tests and lint checks.
4. Run the 15-minute probe.
5. Report the results and your assessment of whether AISStream coverage and vessel
   continuity justify proceeding.

Do not implement Kafka, Flink, PostgreSQL or a dashboard. Do not generate simulated
traffic. Do not commit or push without my approval.
```
