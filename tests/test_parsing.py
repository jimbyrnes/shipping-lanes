"""Parser tests.

The frames built here are hand-written schema fixtures for unit tests only.
They are never written to data/ and never appear in a probe summary.
"""

import json
from datetime import UTC, datetime

import pytest

from aisprobe.parsing import (
    POSITION_TYPES,
    ControlFrame,
    Observation,
    Reject,
    estimate_event_time,
    parse_frame,
    parse_time_utc,
)

TIME_UTC = "2026-10-12 17:03:41.318353 +0000 UTC"


def frame(message_type="PositionReport", body=None, meta=None, **top):
    default_body = {
        "UserID": 636012345,
        "Latitude": 26.57,
        "Longitude": 56.25,
        "Sog": 12.4,
        "Cog": 301.5,
        "TrueHeading": 300,
        "Timestamp": 40,
        "Valid": True,
    }
    default_meta = {
        "MMSI": 636012345,
        "ShipName": "FIXTURE ONLY        ",
        "latitude": 26.57,
        "longitude": 56.25,
        "time_utc": TIME_UTC,
    }
    obj = {
        "MessageType": message_type,
        "MetaData": default_meta if meta is None else meta,
        "Message": {message_type: default_body if body is None else body},
    }
    obj.update(top)
    return json.dumps(obj)


def parse(text) -> Observation:
    _, result = parse_frame(text)
    assert isinstance(result, Observation), result
    return result


def reject(text) -> Reject:
    _, result = parse_frame(text)
    assert isinstance(result, Reject), result
    return result


def test_position_report_fields():
    obs = parse(frame())
    assert obs.is_position and obs.known_type
    assert obs.mmsi == 636012345
    assert (obs.lat, obs.lon) == (26.57, 56.25)
    assert obs.coord_source == "message"
    assert (obs.sog, obs.cog, obs.heading) == (12.4, 301.5, 300)
    assert obs.ais_second == 40 and obs.ais_second_raw == 40
    assert obs.source_time == datetime(2026, 10, 12, 17, 3, 41, 318353, tzinfo=UTC)
    assert obs.source_time_raw == TIME_UTC


@pytest.mark.parametrize("message_type", sorted(POSITION_TYPES))
def test_every_position_type_is_extracted(message_type):
    obs = parse(frame(message_type))
    assert obs.is_position
    assert (obs.mmsi, obs.lat, obs.lon) == (636012345, 26.57, 56.25)


def test_long_range_message_has_no_ais_second():
    body = {"UserID": 1, "Latitude": 12.6, "Longitude": 43.3, "Sog": 9.0, "Cog": 150.0}
    obs = parse(frame("LongRangeAisBroadcastMessage", body=body))
    assert obs.ais_second is None and obs.ais_second_raw is None
    assert obs.sog == 9.0


@pytest.mark.parametrize("keys", [("latitude", "longitude"), ("Latitude", "Longitude")])
def test_metadata_coordinates_in_either_casing(keys):
    meta = {"MMSI": 7, keys[0]: 12.6, keys[1]: 43.3, "time_utc": TIME_UTC}
    obs = parse(frame("ShipStaticData", body={"UserID": 7}, meta=meta))
    assert not obs.is_position
    assert (obs.lat, obs.lon, obs.coord_source) == (12.6, 43.3, "metadata")


def test_falls_back_to_metadata_when_body_position_unavailable():
    obs = parse(frame(body={"UserID": 5, "Latitude": 91.0, "Longitude": 181.0, "Timestamp": 3}))
    assert (obs.lat, obs.lon, obs.coord_source) == (26.57, 56.25, "metadata")


def test_no_coordinates_anywhere():
    obs = parse(frame(body={"UserID": 5, "Latitude": 91, "Longitude": 181}, meta={}))
    assert obs.lat is None and obs.lon is None and obs.coord_source is None


def test_coordinate_mismatch_is_flagged():
    meta = {"latitude": 26.9, "longitude": 56.25}
    assert parse(frame(meta=meta)).coord_mismatch
    assert not parse(frame()).coord_mismatch


def test_sentinels_become_none():
    body = {
        "UserID": 5,
        "Latitude": 26.0,
        "Longitude": 56.0,
        "Sog": 102.3,
        "Cog": 360,
        "TrueHeading": 511,
        "Timestamp": 60,
    }
    obs = parse(frame(body=body))
    assert (obs.sog, obs.cog, obs.heading, obs.ais_second) == (None, None, None, None)
    assert obs.ais_second_raw == 60  # the transponder value is still preserved


def test_wrong_types_do_not_raise():
    body = {"UserID": "abc", "Latitude": "26", "Longitude": None, "Sog": True, "Timestamp": 4.5}
    obs = parse(frame(body=body, meta={"MMSI": "x", "time_utc": 12345}))
    assert obs.mmsi is None and obs.lat is None and obs.sog is None
    assert obs.ais_second is None and obs.source_time is None and obs.source_time_raw is None


def test_mmsi_falls_back_to_metadata():
    obs = parse(frame("ShipStaticData", body={}, meta={"MMSI": 99}))
    assert obs.mmsi == 99


def test_unknown_message_type_is_kept_but_flagged():
    obs = parse(frame("SomethingNew", body={"UserID": 3}))
    assert not obs.known_type and not obs.is_position
    assert obs.lat == 26.57  # still classifiable from MetaData


def test_missing_metadata_is_tolerated():
    text = json.dumps(
        {"MessageType": "PositionReport", "Message": {"PositionReport": {"UserID": 1}}}
    )
    obs = parse(text)
    assert not obs.metadata_present and obs.source_time is None


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("not json {", "invalid_json"),
        ("", "invalid_json"),
        ("[1, 2]", "not_an_object"),
        ('"text"', "not_an_object"),
        ('{"error": "Api Key Is Not Valid"}', "error_frame"),
        ('{"MetaData": {}}', "missing_message_type"),
        ('{"MessageType": 5}', "missing_message_type"),
        ('{"MessageType": "PositionReport", "MetaData": {}}', "missing_message_body"),
        (
            '{"MessageType": "PositionReport", "Message": {"PositionReport": 1}}',
            "missing_message_body",
        ),
        ('{"MessageType": "PositionReport", "Message": []}', "missing_message_body"),
    ],
)
def test_rejects(text, reason):
    assert reject(text).reason == reason


def test_error_frame_keeps_server_text():
    assert reject('{"error": "Api Key Is Not Valid"}').detail == "Api Key Is Not Valid"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2022-12-29 18:22:32.318353 +0000 UTC", datetime(2022, 12, 29, 18, 22, 32, 318353)),
        ("2026-07-12 12:00:00.123456789 +0000 UTC", datetime(2026, 7, 12, 12, 0, 0, 123456)),
        ("2026-07-12 12:00:00.5 +0000 UTC", datetime(2026, 7, 12, 12, 0, 0, 500000)),
        ("2026-07-12 12:00:00 +0000 UTC", datetime(2026, 7, 12, 12, 0, 0)),
        ("2026-07-12T12:00:00Z", datetime(2026, 7, 12, 12, 0, 0)),
        ("2026-07-12 14:00:00 +0200", datetime(2026, 7, 12, 12, 0, 0)),
    ],
)
def test_parse_time_utc(value, expected):
    assert parse_time_utc(value) == expected.replace(tzinfo=UTC)


@pytest.mark.parametrize("value", ["", "yesterday", "2026-13-40 00:00:00 +0000 UTC", None, 17])
def test_parse_time_utc_rejects_garbage(value):
    assert parse_time_utc(value) is None


def test_estimated_event_time_same_minute():
    source = datetime(2026, 10, 12, 17, 3, 41, 300000, tzinfo=UTC)
    assert estimate_event_time(source, 40) == datetime(2026, 10, 12, 17, 3, 40, tzinfo=UTC)


def test_estimated_event_time_crosses_minute_boundary():
    source = datetime(2026, 10, 12, 17, 4, 1, tzinfo=UTC)
    assert estimate_event_time(source, 58) == datetime(2026, 10, 12, 17, 3, 58, tzinfo=UTC)
    source = datetime(2026, 10, 12, 23, 59, 59, tzinfo=UTC)
    assert estimate_event_time(source, 1) == datetime(2026, 10, 13, 0, 0, 1, tzinfo=UTC)


def test_estimated_event_time_needs_both_inputs():
    source = datetime(2026, 10, 12, 17, 4, 1, tzinfo=UTC)
    assert estimate_event_time(source, None) is None
    assert estimate_event_time(None, 10) is None


def test_subscription_confirmation_is_a_control_frame():
    # Shape copied from the live feed (2026-10-12); it has no MetaData.
    text = '{"Message": {"CompressionEnabled": true}, "MessageType": "SubscriptionConfirmation"}'
    _, result = parse_frame(text)
    assert result == ControlFrame("SubscriptionConfirmation")
