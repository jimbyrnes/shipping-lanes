"""Connect to AISStream, record real frames for a fixed window, and summarise coverage."""

import argparse
import asyncio
import contextlib
import json
import os
import random
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus, WebSocketException

from aisprobe.parsing import Observation, Reject, parse_frame
from aisprobe.regions import CONTROL, TARGET_REGIONS, UNCLASSIFIED, Region, classify
from aisprobe.stats import ProbeStats, format_summary, iso

URL = "wss://stream.aisstream.io/v0/stream"
API_KEY_ENV = "AISSTREAM_API_KEY"
OPEN_TIMEOUT_S = 20.0
MAX_FRAME_BYTES = 1 << 20
MAX_BACKOFF_S = 60.0
THROTTLE_MIN_BACKOFF_S = 30.0
MAX_CONSECUTIVE_429 = 3
PROGRESS_INTERVAL_S = 15.0


@dataclass(frozen=True)
class ProbeConfig:
    api_key: str
    regions: list[Region]
    duration_s: float
    stall_timeout_s: float
    max_reconnects: int


def utcnow() -> datetime:
    return datetime.now(UTC)


def make_scrubber(secret: str) -> Callable[[str], str]:
    def scrub(text: str) -> str:
        return text.replace(secret, "[REDACTED]") if secret else text

    return scrub


def log(message: str) -> None:
    print(f"[{utcnow().strftime('%H:%M:%S')}Z] {message}", file=sys.stderr, flush=True)


class RawWriter:
    """Append-only JSONL writer that stops writing, but keeps counting, at a size cap."""

    def __init__(self, path: Path, max_bytes: int) -> None:
        self.path = path
        self.max_bytes = max_bytes
        self.bytes_written = 0
        self.lines_written = 0
        self.cap_reached = False
        self.cap_reached_at: datetime | None = None
        self.frames_not_written = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("w", encoding="utf-8")

    def write(self, record: dict, ts: datetime) -> None:
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        size = len(line.encode("utf-8"))
        if self.cap_reached or self.bytes_written + size > self.max_bytes:
            if not self.cap_reached:
                self.cap_reached = True
                self.cap_reached_at = ts
                log(f"raw size cap of {self.max_bytes} bytes reached; statistics continue")
            self.frames_not_written += 1
            return
        self._file.write(line)
        self.bytes_written += size
        self.lines_written += 1

    def close(self) -> None:
        self._file.close()

    def info(self) -> dict:
        return {
            "path": str(self.path),
            "max_bytes": self.max_bytes,
            "bytes_written": self.bytes_written,
            "lines_written": self.lines_written,
            "cap_reached": self.cap_reached,
            "cap_reached_at": iso(self.cap_reached_at),
            "frames_not_written": self.frames_not_written,
        }


def handle_frame(
    frame: str | bytes,
    conn: int,
    cfg: ProbeConfig,
    stats: ProbeStats,
    raw: RawWriter,
    scrub: Callable[[str], str],
) -> bool:
    """Record one frame. Returns True when the server rejected the API key."""
    ingest = utcnow()
    if isinstance(frame, bytes):
        stats.binary_frames += 1
        text = frame.decode("utf-8", errors="replace")
    else:
        text = frame
    obj, result = parse_frame(text)

    region = None
    if isinstance(result, Observation) and result.lat is not None:
        region = classify(result.lat, result.lon, cfg.regions) or UNCLASSIFIED

    record = {"ingest_ts": iso(ingest), "conn": conn, "region": region}
    if obj is None or cfg.api_key in text:
        record["raw_text"] = scrub(text)
    else:
        record["msg"] = obj
    raw.write(record, ingest)
    stats.record(ingest, obj, result, region)
    return (
        isinstance(result, Reject)
        and result.reason == "error_frame"
        and "api key" in result.detail.lower()
    )


async def receive(
    ws,
    conn: int,
    cfg: ProbeConfig,
    stats: ProbeStats,
    raw: RawWriter,
    scrub: Callable[[str], str],
    deadline: float,
) -> str:
    """Read frames until the deadline, a stall or an auth rejection. Returns the cause."""
    has_control = any(r.is_control for r in cfg.regions)
    last_frame = time.monotonic()
    next_progress = last_frame + PROGRESS_INTERVAL_S
    while True:
        now = time.monotonic()
        if now >= deadline:
            stats.silence(now - last_frame, stalled=False)
            return "deadline"
        if now >= next_progress:
            counts = ", ".join(f"{n}={s.messages}" for n, s in stats.regions.items())
            log(f"{deadline - now:.0f}s left | {counts}")
            next_progress = now + PROGRESS_INTERVAL_S
        silent = now - last_frame
        if silent >= cfg.stall_timeout_s:
            stats.silence(silent, stalled=True)
            log(f"no frames for {silent:.0f}s")
            # Without a control region silence is expected in sparse areas, and
            # reconnecting would only burn connection attempts.
            if has_control:
                return "stall"
            last_frame = now
        timeout = min(deadline - now, cfg.stall_timeout_s - (now - last_frame), 5.0)
        try:
            frame = await asyncio.wait_for(ws.recv(), timeout=max(timeout, 0.01))
        except TimeoutError:
            continue
        stats.silence(time.monotonic() - last_frame, stalled=False)
        last_frame = time.monotonic()
        if handle_frame(frame, conn, cfg, stats, raw, scrub):
            return "auth_rejected"


async def run_probe(
    cfg: ProbeConfig, stats: ProbeStats, raw: RawWriter, scrub: Callable[[str], str]
) -> str:
    """Run until the deadline. Returns the end reason."""
    subscription = json.dumps(
        {"APIKey": cfg.api_key, "BoundingBoxes": [r.bounding_box() for r in cfg.regions]}
    )
    deadline = time.monotonic() + cfg.duration_s
    failures = 0
    throttled = 0

    while time.monotonic() < deadline:
        min_backoff = 0.0
        observed_before = stats.observations
        stats.connect_attempt()
        try:
            ws = await connect(
                URL,
                compression="deflate",
                open_timeout=min(OPEN_TIMEOUT_S, max(1.0, deadline - time.monotonic())),
                max_size=MAX_FRAME_BYTES,
                max_queue=64,
            )
        except InvalidStatus as exc:
            status = exc.response.status_code
            stats.handshake_failed(f"http_{status}")
            log(f"handshake refused with HTTP {status}")
            if status in (401, 403):
                return "auth_rejected"
            if status == 429:
                throttled += 1
                min_backoff = THROTTLE_MIN_BACKOFF_S
                if throttled >= MAX_CONSECUTIVE_429:
                    return "throttled"
        except (OSError, TimeoutError, WebSocketException) as exc:
            stats.handshake_failed(type(exc).__name__, str(exc))
            log(f"connection failed: {type(exc).__name__}")
        else:
            conn = stats.connection_opened(utcnow())
            log(f"connection {conn} open")
            cause = "error"
            try:
                await ws.send(subscription)
                stats.subscription_sent()
                log(f"subscription sent for {[r.name for r in cfg.regions]}")
                cause = await receive(ws, conn, cfg, stats, raw, scrub, deadline)
            except ConnectionClosed:
                cause = "server_closed"
            except (OSError, WebSocketException) as exc:
                log(f"connection error: {type(exc).__name__}")
            except asyncio.CancelledError:
                cause = "interrupted"
                raise
            finally:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(ws.close(), timeout=5.0)
                stats.connection_closed(conn, utcnow(), ws.close_code, ws.close_reason or "", cause)
                log(f"connection {conn} closed ({cause}, code {ws.close_code})")
            if cause == "deadline":
                return "duration_elapsed"
            if cause == "auth_rejected":
                return "auth_rejected"

        if stats.observations > observed_before:
            failures = 0
            throttled = 0
        else:
            failures += 1
            if failures > cfg.max_reconnects:
                return "reconnect_attempts_exhausted"
        delay = min(MAX_BACKOFF_S, 2.0 * 2**failures) * random.uniform(0.5, 1.0)
        delay = min(max(delay, min_backoff), max(0.0, deadline - time.monotonic()))
        if delay > 0:
            log(f"reconnecting in {delay:.1f}s")
            await asyncio.sleep(delay)
    return "duration_elapsed"


async def _run_with_signals(cfg, stats, raw, scrub) -> str:
    task = asyncio.current_task()
    with contextlib.suppress(NotImplementedError, ValueError):
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
    return await run_probe(cfg, stats, raw, scrub)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="aisprobe", description="AISStream coverage probe (real data only)."
    )
    parser.add_argument("--duration", type=float, default=900.0, help="probe length in seconds")
    parser.add_argument(
        "--regions",
        default=",".join(TARGET_REGIONS),
        help=f"comma-separated target regions (available: {', '.join(TARGET_REGIONS)})",
    )
    parser.add_argument(
        "--no-control", action="store_true", help="do not subscribe to the control region"
    )
    parser.add_argument(
        "--gap-threshold", type=float, default=60.0, help="seconds without a position = a gap"
    )
    parser.add_argument("--out", type=Path, default=Path("data"), help="output directory")
    parser.add_argument("--max-raw-mb", type=float, default=200.0, help="raw JSONL size cap")
    parser.add_argument(
        "--stall-timeout", type=float, default=120.0, help="seconds of total silence = a stall"
    )
    parser.add_argument(
        "--max-reconnects", type=int, default=8, help="consecutive failed attempts allowed"
    )
    args = parser.parse_args(argv)
    if args.duration <= 0 or args.gap_threshold <= 0 or args.stall_timeout <= 0:
        parser.error("--duration, --gap-threshold and --stall-timeout must be positive")
    if args.max_raw_mb < 0 or args.max_reconnects < 0:
        parser.error("--max-raw-mb and --max-reconnects must not be negative")
    names = [n.strip() for n in args.regions.split(",") if n.strip()]
    unknown = [n for n in names if n not in TARGET_REGIONS]
    if unknown or not names:
        parser.error(f"unknown or empty --regions: {unknown}")
    args.region_names = list(dict.fromkeys(names))
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    api_key = os.environ.get(API_KEY_ENV, "").strip()
    if not api_key:
        print(f"{API_KEY_ENV} is not set; nothing was sent.", file=sys.stderr)
        return 2

    regions = [TARGET_REGIONS[name] for name in args.region_names]
    if not args.no_control:
        regions.append(CONTROL)
    cfg = ProbeConfig(
        api_key=api_key,
        regions=regions,
        duration_s=args.duration,
        stall_timeout_s=args.stall_timeout,
        max_reconnects=args.max_reconnects,
    )
    scrub = make_scrubber(api_key)
    started_at = utcnow()
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    raw = RawWriter(args.out / "raw" / f"probe-{stamp}.jsonl", int(args.max_raw_mb * 1024 * 1024))
    stats = ProbeStats(regions, started_at, args.gap_threshold, scrub)

    try:
        end_reason = asyncio.run(_run_with_signals(cfg, stats, raw, scrub))
    except (KeyboardInterrupt, asyncio.CancelledError):
        end_reason = "interrupted"
    except Exception as exc:  # noqa: BLE001 - the summary must still be written
        end_reason = "error"
        stats.fatal_error = scrub(f"{type(exc).__name__}: {exc}")[:300]
    finally:
        raw.close()

    summary = stats.build_summary(utcnow(), end_reason, args.duration, raw.info())
    summary_path = args.out / "summaries" / f"probe-{stamp}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(format_summary(summary))
    print(f"\nSummary JSON: {summary_path}")
    return 0 if summary["run"]["completed"] else 1
