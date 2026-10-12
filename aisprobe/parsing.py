"""Parse AISStream frames into observations without ever raising on bad input.

Three clocks are kept apart on purpose:

* ``ais_second``  - the transponder's own field: UTC second of the minute only.
* ``source_time`` - AISStream's ``MetaData.time_utc`` stamp.
* ingest time     - assigned by the probe on receipt (not handled here).

``estimate_event_time`` combines the first two into an *estimate*. It is not a
transmitted or verified event time.
"""

import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

POSITION_TYPES = frozenset(
    {
        "PositionReport",
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
        "LongRangeAisBroadcastMessage",
    }
)

# AisMessageTypes enum from aisstream/ais-message-models.
KNOWN_MESSAGE_TYPES = POSITION_TYPES | frozenset(
    {
        "UnknownMessage",
        "AddressedSafetyMessage",
        "AddressedBinaryMessage",
        "AidsToNavigationReport",
        "AssignedModeCommand",
        "BaseStationReport",
        "BinaryAcknowledge",
        "BinaryBroadcastMessage",
        "ChannelManagement",
        "CoordinatedUTCInquiry",
        "DataLinkManagementMessage",
        "DataLinkManagementMessageData",
        "GroupAssignmentCommand",
        "GnssBroadcastBinaryMessage",
        "Interrogation",
        "MultiSlotBinaryMessage",
        "SafetyBroadcastMessage",
        "ShipStaticData",
        "SingleSlotBinaryMessage",
        "StandardSearchAndRescueAircraftReport",
        "StaticDataReport",
    }
)

# Undocumented service frames seen on the live feed. They carry no MetaData:
# {"MessageType": "SubscriptionConfirmation", "Message": {"CompressionEnabled": true}}
CONTROL_FRAME_TYPES = frozenset({"SubscriptionConfirmation"})

# AIS "not available" sentinels.
SOG_UNAVAILABLE = 102.3
COG_UNAVAILABLE = 360.0
HEADING_MAX = 359
AIS_SECOND_MAX = 59
# Body and MetaData coordinates further apart than this are counted as a mismatch.
COORD_MISMATCH_DEG = 1e-3

_TIME_UTC_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?"
    r"\s*(Z|[+-]\d{2}:?\d{2})?(?:\s+UTC)?$"
)


@dataclass(frozen=True, slots=True)
class Observation:
    message_type: str
    known_type: bool
    is_position: bool
    mmsi: int | None
    lat: float | None
    lon: float | None
    coord_source: str | None  # "message", "metadata" or None
    coord_mismatch: bool
    sog: float | None
    cog: float | None
    heading: int | None
    ais_second: int | None  # transponder second of minute, None when unavailable
    ais_second_raw: int | None  # the Timestamp field exactly as received
    source_time: datetime | None  # parsed MetaData.time_utc
    source_time_raw: str | None  # MetaData.time_utc exactly as received
    metadata_present: bool


@dataclass(frozen=True, slots=True)
class Reject:
    reason: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class ControlFrame:
    """A service frame from AISStream that is not an AIS message."""

    kind: str


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    if number is None or number != int(number):
        return None
    return int(number)


def _coords(container: dict, lat_keys: tuple[str, ...], lon_keys: tuple[str, ...]):
    lat = next((_number(container[k]) for k in lat_keys if k in container), None)
    lon = next((_number(container[k]) for k in lon_keys if k in container), None)
    if lat is None or lon is None:
        return None
    # Out-of-range values include the AIS sentinels (lat 91, lon 181).
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def parse_time_utc(value: Any) -> datetime | None:
    """Parse AISStream's Go-style stamp, e.g. ``2022-12-29 18:22:32.318353 +0000 UTC``."""
    if not isinstance(value, str):
        return None
    match = _TIME_UTC_RE.match(value.strip())
    if not match:
        return None
    year, month, day, hour, minute, second, fraction, offset = match.groups()
    micro = int((fraction or "0")[:6].ljust(6, "0"))
    tz = UTC
    if offset and offset != "Z":
        digits = offset[1:].replace(":", "")
        delta = timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
        tz = timezone(-delta if offset[0] == "-" else delta)
    try:
        parsed = datetime(
            int(year), int(month), int(day), int(hour), int(minute), int(second), micro, tz
        )
    except ValueError:
        return None
    return parsed.astimezone(UTC)


def estimate_event_time(source_time: datetime | None, ais_second: int | None) -> datetime | None:
    """ESTIMATED event time: the minute nearest ``source_time`` that has ``ais_second``.

    By construction the result is within 30 s of ``source_time``, so it cannot
    reveal a delivery delay longer than that. Treat it as an estimate only.
    """
    if source_time is None or ais_second is None:
        return None
    base = source_time.replace(second=ais_second, microsecond=0)
    candidates = (base - timedelta(minutes=1), base, base + timedelta(minutes=1))
    return min(candidates, key=lambda c: abs((c - source_time).total_seconds()))


def parse_frame(text: str) -> tuple[Any, Observation | Reject | ControlFrame]:
    """Return ``(decoded_json_or_None, Observation | Reject | ControlFrame)``."""
    try:
        obj = json.loads(text)
    except (ValueError, RecursionError):
        return None, Reject("invalid_json")
    if not isinstance(obj, dict):
        return obj, Reject("not_an_object")
    if "MessageType" not in obj and ("error" in obj or "Error" in obj):
        return obj, Reject("error_frame", str(obj.get("error", obj.get("Error")))[:200])

    message_type = obj.get("MessageType")
    if not isinstance(message_type, str) or not message_type:
        return obj, Reject("missing_message_type")
    if message_type in CONTROL_FRAME_TYPES:
        return obj, ControlFrame(message_type)

    meta = obj.get("MetaData")
    metadata_present = isinstance(meta, dict)
    if not metadata_present:
        meta = {}

    message = obj.get("Message")
    body = message.get(message_type) if isinstance(message, dict) else None
    if not isinstance(body, dict):
        body = None

    is_position = message_type in POSITION_TYPES
    if is_position and body is None:
        return obj, Reject("missing_message_body", message_type)

    body_coords = _coords(body, ("Latitude",), ("Longitude",)) if body else None
    # MetaData has no official schema; sources disagree on key casing, so accept both.
    meta_coords = _coords(meta, ("latitude", "Latitude"), ("longitude", "Longitude"))
    coords, coord_source = None, None
    if body_coords is not None:
        coords, coord_source = body_coords, "message"
    elif meta_coords is not None:
        coords, coord_source = meta_coords, "metadata"
    mismatch = (
        body_coords is not None
        and meta_coords is not None
        and (
            abs(body_coords[0] - meta_coords[0]) > COORD_MISMATCH_DEG
            or abs(body_coords[1] - meta_coords[1]) > COORD_MISMATCH_DEG
        )
    )

    mmsi = _integer(body.get("UserID")) if body else None
    if mmsi is None:
        mmsi = _integer(meta.get("MMSI"))

    sog = cog = heading = ais_second = ais_second_raw = None
    if is_position:
        sog = _number(body.get("Sog"))
        if sog is not None and not (0.0 <= sog < SOG_UNAVAILABLE):
            sog = None
        cog = _number(body.get("Cog"))
        if cog is not None and not (0.0 <= cog < COG_UNAVAILABLE):
            cog = None
        heading = _integer(body.get("TrueHeading"))
        if heading is not None and not (0 <= heading <= HEADING_MAX):
            heading = None
        ais_second_raw = _integer(body.get("Timestamp"))
        if ais_second_raw is not None and 0 <= ais_second_raw <= AIS_SECOND_MAX:
            ais_second = ais_second_raw

    raw_time = meta.get("time_utc")
    return obj, Observation(
        message_type=message_type,
        known_type=message_type in KNOWN_MESSAGE_TYPES,
        is_position=is_position,
        mmsi=mmsi,
        lat=coords[0] if coords else None,
        lon=coords[1] if coords else None,
        coord_source=coord_source,
        coord_mismatch=mismatch,
        sog=sog,
        cog=cog,
        heading=heading,
        ais_second=ais_second,
        ais_second_raw=ais_second_raw,
        source_time=parse_time_utc(raw_time),
        source_time_raw=raw_time if isinstance(raw_time, str) else None,
        metadata_present=metadata_present,
    )
