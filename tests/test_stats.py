"""Statistics tests, driven by hand-written fixtures that never reach data/."""

import json
from datetime import UTC, datetime, timedelta

from aisprobe import stats as stats_module
from aisprobe.parsing import parse_frame
from aisprobe.probe import ProbeConfig, RawWriter, handle_frame, make_scrubber
from aisprobe.regions import BAB_EL_MANDEB, CONTROL, HORMUZ, classify
from aisprobe.stats import BoundedSample, ProbeStats, format_summary

START = datetime(2026, 10, 12, 17, 0, 0, tzinfo=UTC)
REGIONS = [HORMUZ, BAB_EL_MANDEB, CONTROL]
POINTS = {"hormuz": (26.57, 56.25), "bab_el_mandeb": (12.6, 43.3), "control": (51.95, 4.1)}
RAW_INFO = {
    "path": "unused",
    "max_bytes": 1,
    "bytes_written": 0,
    "lines_written": 0,
    "cap_reached": False,
    "cap_reached_at": None,
    "frames_not_written": 0,
}


def position(mmsi, region, source: datetime, ais_second=None):
    lat, lon = POINTS[region]
    return json.dumps(
        {
            "MessageType": "PositionReport",
            "MetaData": {
                "MMSI": mmsi,
                "time_utc": source.strftime("%Y-%m-%d %H:%M:%S.%f +0000 UTC"),
            },
            "Message": {
                "PositionReport": {
                    "UserID": mmsi,
                    "Latitude": lat,
                    "Longitude": lon,
                    "Timestamp": source.second if ais_second is None else ais_second,
                }
            },
        }
    )


def feed(stats: ProbeStats, offset_s: float, text: str, lag_s: float = 0.0):
    ingest = START + timedelta(seconds=offset_s)
    obj, result = parse_frame(text)
    region = None
    if getattr(result, "lat", None) is not None:
        region = classify(result.lat, result.lon, REGIONS)
    stats.record(ingest + timedelta(seconds=lag_s), obj, result, region)


def run(stats: ProbeStats, seconds: float, reason="duration_elapsed", raw=RAW_INFO):
    return stats.build_summary(START + timedelta(seconds=seconds), reason, seconds, raw)


def test_counts_unique_vessels_and_per_minute_rates():
    stats = ProbeStats(REGIONS, START)
    for offset, mmsi in [(5, 1), (20, 2), (50, 1), (70, 3), (110, 1), (130, 1)]:
        feed(stats, offset, position(mmsi, "hormuz", START + timedelta(seconds=offset)))
    hormuz = run(stats, 180)["regions"]["hormuz"]
    assert hormuz["messages"] == 6 and hormuz["position_observations"] == 6
    assert hormuz["unique_vessels"] == 3
    assert hormuz["messages_per_minute"]["series"] == [3, 2, 1]
    assert hormuz["messages_per_minute"]["peak"] == 3
    assert hormuz["messages_per_minute"]["mean"] == 2.0
    assert hormuz["positions_per_minute"]["empty_full_minutes"] == 0


def test_gap_detection_includes_leading_and_trailing_silence():
    stats = ProbeStats(REGIONS, START, gap_threshold_s=60)
    for offset in (90, 100, 200):  # 90 s lead-in, 100 s inner gap, 100 s tail
        feed(stats, offset, position(1, "hormuz", START + timedelta(seconds=offset)))
    gaps = run(stats, 300)["regions"]["hormuz"]["position_gaps"]
    assert gaps["count_over_threshold"] == 3
    assert gaps["longest_s"] == 100.0


def test_region_with_no_data_is_one_run_long_gap():
    stats = ProbeStats(REGIONS, START, gap_threshold_s=60)
    region = run(stats, 300)["regions"]["bab_el_mandeb"]
    assert region["position_observations"] == 0
    assert region["position_gaps"] == {
        "threshold_s": 60,
        "count_over_threshold": 1,
        "longest_s": 300.0,
        "first_position_at": None,
        "last_position_at": None,
    }
    assert region["positions_per_minute"]["empty_full_minutes"] == 5


def test_unique_vessel_cap(monkeypatch):
    monkeypatch.setattr(stats_module, "MAX_UNIQUE_MMSI", 3)
    stats = ProbeStats(REGIONS, START)
    for mmsi in range(1, 7):
        feed(stats, mmsi, position(mmsi, "hormuz", START))
    hormuz = run(stats, 60)["regions"]["hormuz"]
    assert hormuz["unique_vessels"] == 3 and hormuz["unique_vessels_capped"]
    assert hormuz["messages"] == 6


def test_three_clocks_are_reported_separately():
    stats = ProbeStats(REGIONS, START)
    source = START + timedelta(seconds=61, microseconds=500000)  # 17:01:01.5
    feed(stats, 61.5, position(1, "hormuz", source, ais_second=58), lag_s=2.0)
    ts = run(stats, 120)["regions"]["hormuz"]["timestamps"]
    assert ts["positions_with_ais_second"] == 1
    assert ts["ingest_minus_source_s"]["max"] == 2.0
    # 17:01:01.5 against second 58 can only be estimated as 17:00:58.
    assert ts["source_minus_estimated_event_s"]["max"] == 3.5


def test_out_of_order_source_times_are_counted_per_vessel():
    stats = ProbeStats(REGIONS, START)
    for offset, source_offset in [(1, 10), (2, 5), (3, 20)]:
        feed(stats, offset, position(1, "hormuz", START + timedelta(seconds=source_offset)))
    feed(stats, 4, position(2, "hormuz", START))  # another vessel: not a regression
    ts = run(stats, 60)["regions"]["hormuz"]["timestamps"]
    assert ts["per_vessel_consecutive_pairs"] == 2
    assert ts["source_time_regressions"] == 1


def test_rejects_and_unknown_types_are_counted():
    stats = ProbeStats(REGIONS, START)
    feed(stats, 1, "not json")
    feed(stats, 2, '{"error": "Api Key Is Not Valid"}')
    feed(stats, 3, '{"MessageType": "Mystery", "MetaData": {"latitude": 0, "longitude": 0}}')
    frames = run(stats, 60)["frames"]
    assert frames["total"] == 3 and frames["parsed"] == 1
    assert frames["rejected"] == {"invalid_json": 1, "error_frame": 1}
    assert frames["error_frames"] == ["Api Key Is Not Valid"]
    assert frames["unknown_message_types"] == {"Mystery": 1}


def test_bounded_sample_keeps_exact_extremes():
    sample = BoundedSample(cap=10)
    for value in range(1000):
        sample.add(float(value))
    summary = sample.summary()
    assert (summary["count"], summary["min"], summary["max"]) == (1000, 0.0, 999.0)
    assert summary["percentiles_sampled"] and len(sample._values) == 10
    assert BoundedSample().summary() is None


def test_diagnosis_absent_region_with_control_traffic_is_uncertain():
    stats = ProbeStats(REGIONS, START)
    stats.connection_opened(START)
    feed(stats, 5, position(1, "control", START))
    feed(stats, 6, position(2, "hormuz", START))
    diag = run(stats, 60)["diagnosis"]
    assert diag["control"] == {"enabled": True, "traffic_observed": True, "messages": 1}
    assert "cannot tell these apart" in diag["target_regions"]["bab_el_mandeb"]["assessment"]
    assert "does not show how complete" in diag["target_regions"]["hormuz"]["assessment"]
    assert "control" not in diag["target_regions"]


def test_diagnosis_without_control_traffic_is_inconclusive():
    stats = ProbeStats(REGIONS, START)
    stats.connection_opened(START)
    diag = run(stats, 60)["diagnosis"]
    assert not diag["control"]["traffic_observed"]
    assert "Inconclusive" in diag["target_regions"]["hormuz"]["assessment"]

    no_control = ProbeStats([HORMUZ], START)
    diag = run(no_control, 60)["diagnosis"]
    assert diag["control"]["enabled"] is False
    assert "No control region" in diag["target_regions"]["hormuz"]["assessment"]


def test_websocket_status_is_separate_from_data():
    stats = ProbeStats(REGIONS, START)
    assert run(stats, 60)["diagnosis"]["websocket"]["status"] == "failed"
    conn = stats.connection_opened(START)
    assert run(stats, 60)["diagnosis"]["websocket"]["status"] == "healthy"
    stats.connection_closed(conn, START + timedelta(seconds=30), 1006, "", "server_closed")
    summary = run(stats, 60)
    assert summary["diagnosis"]["websocket"]["status"] == "degraded"
    assert summary["connection"]["interruptions"] == 1
    assert summary["connection"]["connected_s"] == 30.0
    assert summary["connection"]["not_connected_s"] == 30.0


def test_incomplete_run_is_flagged():
    stats = ProbeStats(REGIONS, START)
    summary = stats.build_summary(START + timedelta(seconds=20), "interrupted", 900, RAW_INFO)
    assert summary["run"]["completed"] is False
    assert summary["run"]["end_reason"] == "interrupted"
    assert "INCOMPLETE" in summary["diagnosis"]["caveats"][0]
    assert "INCOMPLETE RUN" in format_summary(summary)
    assert "INCOMPLETE" not in format_summary(run(ProbeStats(REGIONS, START), 60))


def test_raw_cap_stops_writing_but_not_counting(tmp_path):
    secret = "s3cr3t-key"
    cfg = ProbeConfig(secret, REGIONS, 60, 120, 8)
    scrub = make_scrubber(secret)
    line = position(1, "hormuz", START)
    raw = RawWriter(tmp_path / "raw" / "probe.jsonl", max_bytes=2 * (len(line) + 120))
    stats = ProbeStats(REGIONS, START, scrub=scrub)
    for _ in range(5):
        handle_frame(line, 1, cfg, stats, raw, scrub)
    handle_frame(f"garbage {secret}".encode(), 1, cfg, stats, raw, scrub)
    raw.close()

    info = raw.info()
    assert info["cap_reached"] and info["cap_reached_at"] is not None
    assert info["lines_written"] == 2 and info["frames_not_written"] == 4
    written = [json.loads(row) for row in raw.path.read_text().splitlines()]
    assert all(row["region"] == "hormuz" and row["conn"] == 1 for row in written)
    assert written[0]["msg"] == json.loads(line)
    assert written[0]["ingest_ts"].endswith("Z")

    summary = run(stats, 60, raw=info)
    assert summary["regions"]["hormuz"]["messages"] == 5  # statistics kept going
    assert summary["frames"]["total"] == 6 and summary["frames"]["binary"] == 1
    assert "REACHED" in format_summary(summary)


def test_secret_is_never_written_to_raw_file(tmp_path):
    secret = "s3cr3t-key"
    cfg = ProbeConfig(secret, REGIONS, 60, 120, 8)
    scrub = make_scrubber(secret)
    raw = RawWriter(tmp_path / "probe.jsonl", max_bytes=10_000)
    stats = ProbeStats(REGIONS, START, scrub=scrub)
    assert handle_frame(
        json.dumps({"error": f"Api Key Is Not Valid: {secret}"}), 1, cfg, stats, raw, scrub
    )
    handle_frame(f"garbage {secret}", 1, cfg, stats, raw, scrub)
    raw.close()
    assert secret not in raw.path.read_text()
    assert secret not in json.dumps(run(stats, 60, raw=raw.info()))


def test_subscription_confirmation_is_not_counted_as_traffic():
    stats = ProbeStats(REGIONS, START)
    stats.connection_opened(START)
    feed(
        stats,
        1,
        '{"Message": {"CompressionEnabled": true}, "MessageType": "SubscriptionConfirmation"}',
    )
    summary = run(stats, 60)
    assert summary["frames"]["control_frames"] == {"SubscriptionConfirmation": 1}
    assert summary["frames"]["parsed"] == 0 and summary["frames"]["unknown_message_types"] == {}
    assert summary["regions"]["unclassified"]["messages"] == 0
    assert summary["diagnosis"]["websocket"]["subscriptions_confirmed"] == 1
