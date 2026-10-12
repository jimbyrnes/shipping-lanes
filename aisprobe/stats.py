"""Bounded-memory statistics and the coverage summary.

Nothing here grows with message volume: sets and dicts are capped, lag samples
are fixed-size reservoirs, and per-minute buckets are bounded by run length.
"""

import random
from collections import Counter, OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from aisprobe.parsing import ControlFrame, Observation, Reject, estimate_event_time
from aisprobe.regions import UNCLASSIFIED, Region

MAX_UNIQUE_MMSI = 50_000
MAX_TRACKED_VESSELS = 20_000
MAX_DISTINCT_KEYS = 64
MAX_CONNECTION_RECORDS = 200
SAMPLE_CAP = 5_000
OTHER = "(other)"


def iso(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    return ts.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _count_capped(counter: Counter, key: str) -> None:
    if key in counter or len(counter) < MAX_DISTINCT_KEYS:
        counter[key] += 1
    else:
        counter[OTHER] += 1


class BoundedSample:
    """Exact count/min/max/mean plus a fixed-size reservoir for percentiles."""

    def __init__(self, cap: int = SAMPLE_CAP, seed: int = 0) -> None:
        self.cap = cap
        self.count = 0
        self.total = 0.0
        self.min: float | None = None
        self.max: float | None = None
        self._values: list[float] = []
        self._rng = random.Random(seed)

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.min = value if self.min is None else min(self.min, value)
        self.max = value if self.max is None else max(self.max, value)
        if len(self._values) < self.cap:
            self._values.append(value)
        else:
            slot = self._rng.randrange(self.count)
            if slot < self.cap:
                self._values[slot] = value

    def percentile(self, p: float) -> float | None:
        if not self._values:
            return None
        ordered = sorted(self._values)
        return ordered[min(len(ordered) - 1, int(p / 100.0 * len(ordered)))]

    def summary(self) -> dict[str, Any] | None:
        if self.count == 0:
            return None
        return {
            "count": self.count,
            "min": round(self.min, 3),
            "p50": round(self.percentile(50), 3),
            "p95": round(self.percentile(95), 3),
            "max": round(self.max, 3),
            "mean": round(self.total / self.count, 3),
            "percentiles_sampled": self.count > self.cap,
        }


@dataclass
class RegionStats:
    name: str
    is_control: bool = False
    messages: int = 0
    positions: int = 0
    by_type: Counter = field(default_factory=Counter)
    mmsis: set = field(default_factory=set)
    mmsi_overflow: bool = False
    msgs_per_minute: dict = field(default_factory=dict)
    pos_per_minute: dict = field(default_factory=dict)
    first_position: datetime | None = None
    last_position: datetime | None = None
    inner_gap_count: int = 0
    inner_gap_longest_s: float = 0.0
    with_ais_second: int = 0
    with_source_time: int = 0
    with_estimated_event_time: int = 0
    coord_mismatches: int = 0
    coords_from_metadata: int = 0
    ordered_pairs: int = 0
    source_time_regressions: int = 0
    estimated_event_time_regressions: int = 0
    ingest_minus_source: BoundedSample = field(default_factory=BoundedSample)
    source_minus_estimated: BoundedSample = field(default_factory=BoundedSample)


class ProbeStats:
    def __init__(
        self,
        regions: list[Region],
        started_at: datetime,
        gap_threshold_s: float = 60.0,
        scrub: Callable[[str], str] = lambda s: s,
    ) -> None:
        self.region_defs = list(regions)
        self.started_at = started_at
        self.gap_threshold_s = gap_threshold_s
        self.scrub = scrub
        self.regions: dict[str, RegionStats] = {
            r.name: RegionStats(r.name, r.is_control) for r in regions
        }
        self.regions[UNCLASSIFIED] = RegionStats(UNCLASSIFIED)

        self.frames = 0
        self.binary_frames = 0
        self.observations = 0
        self.rejects: Counter = Counter()
        self.error_frames: list[str] = []
        self.unknown_types: Counter = Counter()
        self.control_frames: Counter = Counter()
        self.positions_without_coordinates = 0
        self.metadata_missing = 0
        self.time_utc_unparsed = 0

        # Schema evidence gathered from real frames.
        self.top_level_keys: Counter = Counter()
        self.metadata_keys: Counter = Counter()
        self.body_keys: dict[str, Counter] = {}
        self.time_utc_examples: list[str] = []

        self._vessels: OrderedDict = OrderedDict()

        self.connect_attempts = 0
        self.handshake_failures: Counter = Counter()
        self.handshake_failure_details: list[str] = []
        self.connections: list[dict[str, Any]] = []
        self.connections_opened = 0
        self.subscriptions_sent = 0
        self.connected_seconds = 0.0
        self.first_connected_at: datetime | None = None
        self.interruptions = 0
        self.stalls = 0
        self.longest_silence_s = 0.0
        self.fatal_error: str | None = None

    # -- frames ---------------------------------------------------------------

    def record(
        self,
        ingest: datetime,
        obj: Any,
        result: Observation | Reject | ControlFrame,
        region: str | None,
    ) -> None:
        self.frames += 1
        if isinstance(result, ControlFrame):
            _count_capped(self.control_frames, result.kind)
            return
        if isinstance(result, Reject):
            self.rejects[result.reason] += 1
            if result.reason == "error_frame" and len(self.error_frames) < 5:
                self.error_frames.append(self.scrub(result.detail))
            return

        obs = result
        self.observations += 1
        self._observe_schema(obj, obs)
        if not obs.known_type:
            _count_capped(self.unknown_types, obs.message_type[:60])
        if not obs.metadata_present:
            self.metadata_missing += 1
        if obs.source_time_raw is not None and obs.source_time is None:
            self.time_utc_unparsed += 1

        stats = self.regions.get(region or UNCLASSIFIED, self.regions[UNCLASSIFIED])
        minute = max(0, int((ingest - self.started_at).total_seconds() // 60))
        stats.messages += 1
        _count_capped(stats.by_type, obs.message_type[:60])
        stats.msgs_per_minute[minute] = stats.msgs_per_minute.get(minute, 0) + 1
        if obs.mmsi is not None:
            if obs.mmsi in stats.mmsis or len(stats.mmsis) < MAX_UNIQUE_MMSI:
                stats.mmsis.add(obs.mmsi)
            else:
                stats.mmsi_overflow = True
        if obs.coord_mismatch:
            stats.coord_mismatches += 1
        if obs.coord_source == "metadata":
            stats.coords_from_metadata += 1
        if obs.source_time is not None:
            stats.with_source_time += 1
            stats.ingest_minus_source.add((ingest - obs.source_time).total_seconds())
        if obs.is_position:
            self._record_position(ingest, obs, stats, minute)

    def _record_position(
        self, ingest: datetime, obs: Observation, stats: RegionStats, minute: int
    ) -> None:
        if obs.lat is None:
            self.positions_without_coordinates += 1
        stats.positions += 1
        stats.pos_per_minute[minute] = stats.pos_per_minute.get(minute, 0) + 1
        if stats.last_position is not None:
            gap = (ingest - stats.last_position).total_seconds()
            stats.inner_gap_longest_s = max(stats.inner_gap_longest_s, gap)
            if gap > self.gap_threshold_s:
                stats.inner_gap_count += 1
        else:
            stats.first_position = ingest
        stats.last_position = ingest

        if obs.ais_second is not None:
            stats.with_ais_second += 1
        estimated = estimate_event_time(obs.source_time, obs.ais_second)
        if estimated is not None:
            stats.with_estimated_event_time += 1
            stats.source_minus_estimated.add((obs.source_time - estimated).total_seconds())

        if obs.mmsi is None or obs.source_time is None:
            return
        previous = self._vessels.get(obs.mmsi)
        if previous is not None:
            stats.ordered_pairs += 1
            if obs.source_time < previous[0]:
                stats.source_time_regressions += 1
            if estimated is not None and previous[1] is not None and estimated < previous[1]:
                stats.estimated_event_time_regressions += 1
        self._vessels[obs.mmsi] = (obs.source_time, estimated)
        self._vessels.move_to_end(obs.mmsi)
        if len(self._vessels) > MAX_TRACKED_VESSELS:
            self._vessels.popitem(last=False)

    def _observe_schema(self, obj: Any, obs: Observation) -> None:
        if not isinstance(obj, dict):
            return
        for key in obj:
            _count_capped(self.top_level_keys, str(key)[:60])
        meta = obj.get("MetaData")
        if isinstance(meta, dict):
            for key in meta:
                _count_capped(self.metadata_keys, str(key)[:60])
        if obs.source_time_raw is not None and len(self.time_utc_examples) < 3:
            self.time_utc_examples.append(obs.source_time_raw[:80])
        message = obj.get("Message")
        body = message.get(obs.message_type) if isinstance(message, dict) else None
        if isinstance(body, dict) and (
            obs.message_type in self.body_keys or len(self.body_keys) < MAX_DISTINCT_KEYS
        ):
            counter = self.body_keys.setdefault(obs.message_type, Counter())
            for key in body:
                _count_capped(counter, str(key)[:60])

    # -- connection -----------------------------------------------------------

    def connect_attempt(self) -> None:
        self.connect_attempts += 1

    def handshake_failed(self, kind: str, detail: str = "") -> None:
        _count_capped(self.handshake_failures, kind)
        self.interruptions += 1
        if detail and len(self.handshake_failure_details) < 5:
            self.handshake_failure_details.append(self.scrub(detail)[:200])

    def connection_opened(self, ts: datetime) -> int:
        self.connections_opened += 1
        if self.first_connected_at is None:
            self.first_connected_at = ts
        if len(self.connections) < MAX_CONNECTION_RECORDS:
            self.connections.append(
                {
                    "conn": self.connections_opened,
                    "opened_at": ts,
                    "frames_at_open": self.frames,
                }
            )
        return self.connections_opened

    def subscription_sent(self) -> None:
        self.subscriptions_sent += 1

    def connection_closed(
        self, conn: int, ts: datetime, code: int | None, reason: str, cause: str
    ) -> None:
        # Only an ended run closes a connection without it being an interruption.
        if cause not in ("deadline", "interrupted"):
            self.interruptions += 1
        for record in reversed(self.connections):
            if record["conn"] == conn:
                self.connected_seconds += (ts - record["opened_at"]).total_seconds()
                record.update(
                    closed_at=ts,
                    close_code=code,
                    close_reason=self.scrub(reason)[:200],
                    cause=cause,
                    frames=self.frames - record.pop("frames_at_open"),
                )
                break

    def silence(self, seconds: float, stalled: bool) -> None:
        self.longest_silence_s = max(self.longest_silence_s, seconds)
        if stalled:
            self.stalls += 1

    # -- summary --------------------------------------------------------------

    def _region_summary(self, stats: RegionStats, ended_at: datetime) -> dict[str, Any]:
        elapsed = max((ended_at - self.started_at).total_seconds(), 1e-9)
        minutes = elapsed / 60.0
        full_minutes = max(1, int(elapsed // 60))

        # Gaps between position observations, including the stretch before the
        # first and after the last one. A region with none is one run-long gap.
        if stats.first_position is None:
            edges = [elapsed]
        else:
            edges = [
                (stats.first_position - self.started_at).total_seconds(),
                (ended_at - stats.last_position).total_seconds(),
            ]
        gap_count = stats.inner_gap_count + sum(1 for g in edges if g > self.gap_threshold_s)
        longest = max([stats.inner_gap_longest_s, *edges])

        positions = stats.positions
        return {
            "is_control": stats.is_control,
            "messages": stats.messages,
            "position_observations": positions,
            "unique_vessels": len(stats.mmsis),
            "unique_vessels_capped": stats.mmsi_overflow,
            "message_types": dict(stats.by_type.most_common()),
            "messages_per_minute": {
                "mean": round(stats.messages / minutes, 2),
                "peak": max(stats.msgs_per_minute.values(), default=0),
                "series": [stats.msgs_per_minute.get(m, 0) for m in range(full_minutes)],
            },
            "positions_per_minute": {
                "mean": round(positions / minutes, 2),
                "peak": max(stats.pos_per_minute.values(), default=0),
                "empty_full_minutes": sum(
                    1 for m in range(full_minutes) if not stats.pos_per_minute.get(m)
                ),
                "full_minutes": full_minutes,
            },
            "position_gaps": {
                "threshold_s": self.gap_threshold_s,
                "count_over_threshold": gap_count,
                "longest_s": round(longest, 1),
                "first_position_at": iso(stats.first_position),
                "last_position_at": iso(stats.last_position),
            },
            "timestamps": {
                "positions_with_ais_second": stats.with_ais_second,
                "messages_with_source_time": stats.with_source_time,
                "positions_with_estimated_event_time": stats.with_estimated_event_time,
                "ingest_minus_source_s": stats.ingest_minus_source.summary(),
                "source_minus_estimated_event_s": stats.source_minus_estimated.summary(),
                "per_vessel_consecutive_pairs": stats.ordered_pairs,
                "source_time_regressions": stats.source_time_regressions,
                "estimated_event_time_regressions": stats.estimated_event_time_regressions,
            },
            "coordinates": {
                "taken_from_metadata": stats.coords_from_metadata,
                "body_metadata_mismatches": stats.coord_mismatches,
            },
        }

    def _diagnosis(self, regions: dict[str, Any], completed: bool) -> dict[str, Any]:
        if self.connections_opened == 0:
            ws_status = "failed"
        elif self.interruptions or self.stalls or self.error_frames:
            ws_status = "degraded"
        else:
            ws_status = "healthy"

        control = [r for r in self.region_defs if r.is_control]
        control_messages = sum(regions[r.name]["messages"] for r in control)
        control_seen = control_messages > 0

        assessments = {}
        for region in self.region_defs:
            if region.is_control:
                continue
            summary = regions[region.name]
            if summary["position_observations"]:
                text = (
                    "Position observations were received in this window. That shows some "
                    "coverage at this time; it does not show how complete the coverage is."
                )
            elif summary["messages"]:
                text = (
                    "Only non-position messages were received in this window. A receiver "
                    "appears to be reporting from the area, but no vessel positions were seen."
                )
            elif control_seen:
                text = (
                    "No messages observed. Control traffic arrived on the same connection, so "
                    "the feed was delivering data. The absence is consistent with no receiver "
                    "coverage, but equally with a receiver outage, low traffic in this window, "
                    "or vessels not transmitting. This run cannot tell these apart."
                )
            elif control:
                text = (
                    "No messages observed, and no control traffic either. Inconclusive: the "
                    "feed may not have been delivering data at all (upstream fault, throttling "
                    "or a subscription problem), so nothing can be said about this region."
                )
            else:
                text = (
                    "No messages observed. No control region was subscribed, so a silent feed "
                    "cannot be ruled out. Inconclusive."
                )
            assessments[region.name] = {
                "messages": summary["messages"],
                "position_observations": summary["position_observations"],
                "assessment": text,
            }

        caveats = [
            "A single window is a snapshot; coverage varies with time of day and receiver uptime.",
            (
                "An open WebSocket does not prove data delivery: this feed has had outages where "
                "connections stayed open and nothing arrived."
            ),
            "Absence of AIS data is not absence of ships.",
        ]
        if not completed:
            caveats.insert(0, "This run is INCOMPLETE; every figure covers a partial window.")
        return {
            "websocket": {
                "status": ws_status,
                "connections_opened": self.connections_opened,
                "subscriptions_sent": self.subscriptions_sent,
                "frames_received": self.frames,
                "subscriptions_confirmed": self.control_frames.get("SubscriptionConfirmation", 0),
                "error_frames": self.rejects.get("error_frame", 0),
            },
            "control": {
                "enabled": bool(control),
                "traffic_observed": control_seen,
                "messages": control_messages,
            },
            "target_regions": assessments,
            "caveats": caveats,
        }

    def build_summary(
        self,
        ended_at: datetime,
        end_reason: str,
        requested_duration_s: float,
        raw_capture: dict[str, Any],
    ) -> dict[str, Any]:
        elapsed = (ended_at - self.started_at).total_seconds()
        completed = end_reason == "duration_elapsed"
        regions = {name: self._region_summary(s, ended_at) for name, s in self.regions.items()}
        to_first = (
            (self.first_connected_at - self.started_at).total_seconds()
            if self.first_connected_at
            else None
        )
        connections = [
            {k: iso(v) if isinstance(v, datetime) else v for k, v in record.items()}
            for record in self.connections
        ]
        return {
            "run": {
                "started_at": iso(self.started_at),
                "ended_at": iso(ended_at),
                "requested_duration_s": requested_duration_s,
                "actual_duration_s": round(elapsed, 1),
                "completed": completed,
                "end_reason": end_reason,
                "fatal_error": self.fatal_error,
            },
            "subscription": {r.name: r.bounding_box() for r in self.region_defs},
            "connection": {
                "connect_attempts": self.connect_attempts,
                "connections_opened": self.connections_opened,
                "subscriptions_sent": self.subscriptions_sent,
                "interruptions": self.interruptions,
                "handshake_failures": dict(self.handshake_failures),
                "handshake_failure_details": self.handshake_failure_details,
                "stalls": self.stalls,
                "longest_silence_s": round(self.longest_silence_s, 1),
                "time_to_first_connect_s": None if to_first is None else round(to_first, 2),
                "connected_s": round(self.connected_seconds, 1),
                "not_connected_s": round(max(0.0, elapsed - self.connected_seconds), 1),
                "connections": connections,
            },
            "raw_capture": raw_capture,
            "frames": {
                "total": self.frames,
                "binary": self.binary_frames,
                "parsed": self.observations,
                "rejected": dict(self.rejects),
                "error_frames": self.error_frames,
                "control_frames": dict(self.control_frames),
                "unknown_message_types": dict(self.unknown_types),
                "positions_without_coordinates": self.positions_without_coordinates,
            },
            "regions": regions,
            "schema_observed": {
                "top_level_keys": dict(self.top_level_keys),
                "metadata_keys": dict(self.metadata_keys),
                "metadata_missing": self.metadata_missing,
                "time_utc_examples": self.time_utc_examples,
                "time_utc_unparsed": self.time_utc_unparsed,
                "message_body_keys": {t: sorted(c) for t, c in self.body_keys.items()},
            },
            "timestamp_notes": [
                (
                    "ais_second is the transponder's UTC second of the minute; it carries no "
                    "minute, hour or date."
                ),
                (
                    "source_time is AISStream's MetaData.time_utc; what it measures is not "
                    "documented and is treated here as a receive-side stamp."
                ),
                "ingest_time is this probe's local clock on receipt.",
                (
                    "Estimated event time snaps source_time to the nearest minute matching "
                    "ais_second. It is always within 30 s of source_time by construction, so it "
                    "is an ESTIMATE, not verified event time, and is not a basis for watermark "
                    "settings."
                ),
            ],
            "diagnosis": self._diagnosis(regions, completed),
        }


def _lag(sample: dict[str, Any] | None) -> str:
    if not sample:
        return "n/a"
    return f"p50 {sample['p50']}s, p95 {sample['p95']}s, max {sample['max']}s (n={sample['count']})"


def format_summary(summary: dict[str, Any]) -> str:
    run, conn, raw = summary["run"], summary["connection"], summary["raw_capture"]
    diag = summary["diagnosis"]
    lines = ["", "AISStream coverage probe summary", "=" * 32]
    if not run["completed"]:
        lines.append(
            f"*** INCOMPLETE RUN: ended by '{run['end_reason']}' after "
            f"{run['actual_duration_s']}s of {run['requested_duration_s']}s requested ***"
        )
    lines += [
        (
            f"Window: {run['started_at']} to {run['ended_at']} "
            f"({run['actual_duration_s']}s, end reason: {run['end_reason']})"
        ),
        (
            f"WebSocket: {diag['websocket']['status']} | opened {conn['connections_opened']} of "
            f"{conn['connect_attempts']} attempts | subscriptions sent {conn['subscriptions_sent']}, "
            f"confirmed {diag['websocket']['subscriptions_confirmed']} "
            f"| interruptions {conn['interruptions']} | stalls {conn['stalls']} "
            f"| not connected {conn['not_connected_s']}s"
        ),
    ]
    if conn["handshake_failures"]:
        lines.append(f"Handshake failures: {conn['handshake_failures']}")
    if run["fatal_error"]:
        lines.append(f"Fatal error: {run['fatal_error']}")
    frames = summary["frames"]
    lines.append(
        f"Frames: {frames['total']} received, {frames['parsed']} parsed, "
        f"rejected {frames['rejected'] or 'none'}"
    )
    if frames["error_frames"]:
        lines.append(f"Server error frames: {frames['error_frames']}")
    cap = "REACHED - later frames not saved" if raw["cap_reached"] else "not reached"
    lines.append(
        f"Raw file: {raw['path']} | {raw['lines_written']} lines, {raw['bytes_written']} bytes "
        f"| size cap {raw['max_bytes']} bytes: {cap}"
    )
    if raw["cap_reached"]:
        lines.append(
            f"  cap reached at {raw['cap_reached_at']}; {raw['frames_not_written']} frames "
            "counted in statistics but not saved"
        )

    lines.append("")
    for name, region in summary["regions"].items():
        if name == "unclassified" and not region["messages"]:
            continue
        label = f"{name} (control)" if region["is_control"] else name
        ppm, gaps, ts = (
            region["positions_per_minute"],
            region["position_gaps"],
            region["timestamps"],
        )
        lines += [
            f"[{label}]",
            (
                f"  messages {region['messages']} | positions {region['position_observations']} "
                f"| unique vessels {region['unique_vessels']}"
            ),
            (
                f"  messages/min mean {region['messages_per_minute']['mean']}, "
                f"peak {region['messages_per_minute']['peak']} | positions/min mean {ppm['mean']}, "
                f"peak {ppm['peak']}"
            ),
            (
                f"  position gaps > {gaps['threshold_s']}s: {gaps['count_over_threshold']} "
                f"| longest {gaps['longest_s']}s | empty minutes {ppm['empty_full_minutes']} "
                f"of {ppm['full_minutes']}"
            ),
        ]
        if region["messages"]:
            lines += [
                f"  types: {region['message_types']}",
                (
                    f"  ais_second present on {ts['positions_with_ais_second']} of "
                    f"{region['position_observations']} positions"
                ),
                f"  ingest - source_time: {_lag(ts['ingest_minus_source_s'])}",
                (
                    f"  source_time - ESTIMATED event time: "
                    f"{_lag(ts['source_minus_estimated_event_s'])}"
                ),
                (
                    f"  per-vessel source_time regressions: {ts['source_time_regressions']} of "
                    f"{ts['per_vessel_consecutive_pairs']} consecutive pairs"
                ),
            ]

    lines += ["", "Diagnosis (what was observed, not a verdict)"]
    control = diag["control"]
    if control["enabled"]:
        seen = "observed" if control["traffic_observed"] else "NOT observed"
        lines.append(f"  control traffic: {seen} ({control['messages']} messages)")
    else:
        lines.append("  control traffic: no control region subscribed")
    for name, item in diag["target_regions"].items():
        lines.append(f"  {name}: {item['assessment']}")
    lines.append("Caveats")
    lines += [f"  - {c}" for c in diag["caveats"]]
    return "\n".join(lines)
