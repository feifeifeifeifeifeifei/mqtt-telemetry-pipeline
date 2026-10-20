"""Turn a raw MQTT payload into validated readings, or into recorded rejections.

Validation is positive: a reading is kept only if it proves to be a finite number
of numeric type inside a physically plausible range. Everything else is rejected
by the same path, so a fault shape nobody has seen takes the route the known ones
take. A blocklist of known-bad values is always one fault behind: the next
firmware finds a way to be wrong that nobody listed.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# Physical impossibility, NOT operational health. A CPU at 95 C is alarming but
# real and must reach the anomaly detector; a CPU at 255 C is a corrupt reading
# and must not. Operational thresholds live in anomalies.RULES. Keeping the two
# apart means widening an alert never silently starts discarding data.
PLAUSIBLE_RANGES: dict[str, tuple[float, float]] = {
    "battery_percent": (0.0, 100.0),
    "cpu_temperature_c": (-20.0, 120.0),
    "cpu_usage_percent": (0.0, 100.0),
    "disk_usage_percent": (0.0, 100.0),
    "gpu_temperature_c": (-20.0, 120.0),
    "memory_total_mb": (0.0, 1_048_576.0),
    "memory_used_mb": (0.0, 1_048_576.0),
    "memory_used_percent": (0.0, 100.0),
    "network_rx_kbps": (0.0, 10_000_000.0),
    "network_tx_kbps": (0.0, 10_000_000.0),
    "uptime_seconds": (0.0, 31_536_000.0),
}

REJECT_MISSING = "missing"
REJECT_NULL = "null"
REJECT_NOT_A_NUMBER = "not-a-number"
REJECT_NOT_FINITE = "not-finite"
REJECT_OUT_OF_RANGE = "out-of-range"
# Must always be zero. A non-zero count means this module has a branch that
# rejects without naming a reason - a bug here, not a new fault in the stream.
REJECT_OTHER = "other"

REASON_UNDECODABLE = "undecodable"
REASON_WRONG_SHAPE = "wrong-shape"

SOURCE_PAYLOAD = "payload"
SOURCE_UNPARSEABLE = "receipt-unparseable"
SOURCE_IMPLAUSIBLE = "receipt-implausible"

FLAG_MEMORY_INCONSISTENT = "memory-inconsistent"
FLAG_UPTIME_REGRESSED = "uptime-regressed"

MEMORY_TOLERANCE_POINTS = 1.5
MAX_RAW_REPR = 60

# Read once at import, not per message: a malformed value then fails loudly at
# startup instead of raising inside the pipeline, where ingest's except-clause
# would turn it into a silently rising handler_errors count with every message
# dropped - a service that looks healthy while it discards everything.
MAX_CLOCK_SKEW_SECONDS: float = float(os.environ.get("MAX_CLOCK_SKEW_SECONDS", "60"))


class PayloadError(Exception):
    """The payload could not be turned into a JSON object at all.

    reason is "undecodable" (not JSON) or "wrong-shape" (valid JSON, but an
    array/string/number/null rather than an object).
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class Rejection:
    field: str
    reason: str
    raw: str


@dataclass(frozen=True)
class ValidatedMessage:
    readings: dict[str, float]
    rejections: tuple[Rejection, ...]
    timestamp: datetime
    timestamp_source: str
    received_mono: float
    sequence: int | None
    device_id: str | None
    consistency_flags: tuple[str, ...]


def isoformat_z(moment: datetime) -> str:
    """Render a UTC datetime as `...Z`, the form every documented example uses.

    datetime.isoformat() emits `+00:00`, which would make every live response
    differ cosmetically from the documented examples.
    """
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _reason_for(field: str, message: dict[str, Any]) -> str | None:
    """Why this field is unusable, or None if it is fine.

    The order matters. The type check must precede any comparison, because
    `0 <= "97.64" <= 100` raises TypeError in Python 3 rather than returning False.
    """
    if field not in message:
        return REJECT_MISSING
    value = message[field]
    if value is None:
        return REJECT_NULL
    # `type(...) in` rather than isinstance: bool subclasses int, so
    # isinstance(True, int) is True and `true` would be kept as 1.0.
    if type(value) not in (int, float):
        return REJECT_NOT_A_NUMBER
    try:
        finite = math.isfinite(value)
    except OverflowError:
        # math.isfinite raises on an int too large to convert to float, e.g.
        # 10**400. JSON has no integer bound, so this is reachable from the wire.
        return REJECT_NOT_FINITE
    if not finite:
        return REJECT_NOT_FINITE
    low, high = PLAUSIBLE_RANGES[field]
    if not low <= value <= high:
        return REJECT_OUT_OF_RANGE
    return None


def parse_timestamp(value: Any, received_at: datetime) -> tuple[datetime, str]:
    """Parse the payload timestamp, falling back to when we received it.

    Device clocks send timestamps that do not parse at all and others that
    parse cleanly and are still hours wrong, and top-temperatures must report a
    timestamp per reading, so a fallback is mandatory. The source is reported
    alongside so a caller can tell a substituted value from a real one.
    """
    # isinstance first: datetime.fromisoformat(17) raises TypeError, not
    # ValueError, and (17).replace(...) raises AttributeError.
    if not isinstance(value, str) or not value:
        return received_at, SOURCE_UNPARSEABLE
    try:
        # No .replace("Z", "+00:00"): fromisoformat has handled a trailing Z
        # natively since 3.11, and this project targets 3.11+. Mutation testing
        # is what surfaced it - two mutants of that call survived, which is the
        # signature of a line that cannot affect behaviour.
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return received_at, SOURCE_UNPARSEABLE
    if parsed.tzinfo is None:
        # Attach UTC before any arithmetic: subtracting a naive datetime from an
        # aware one raises TypeError.
        parsed = parsed.replace(tzinfo=timezone.utc)
    if abs((parsed - received_at).total_seconds()) > MAX_CLOCK_SKEW_SECONDS:
        return received_at, SOURCE_IMPLAUSIBLE
    # Normalise: a payload carrying +02:00 would otherwise be stored verbatim and
    # serialise as "+02:00" rather than the "Z" every documented example shows.
    return parsed.astimezone(timezone.utc), SOURCE_PAYLOAD


def _consistency_flags(
    readings: dict[str, float], previous_uptime: float | None
) -> tuple[str, ...]:
    """Cross-field checks: structurally valid values that are wrong relative to each other.

    Both flag rather than substitute, because neither has a trusted alternative
    the way the timestamp has our own receipt clock.
    """
    flags: list[str] = []
    used = readings.get("memory_used_mb")
    total = readings.get("memory_total_mb")
    percent = readings.get("memory_used_percent")
    if used is not None and total is not None and percent is not None and total > 0:
        if abs(used / total * 100.0 - percent) > MEMORY_TOLERANCE_POINTS:
            flags.append(FLAG_MEMORY_INCONSISTENT)
    uptime = readings.get("uptime_seconds")
    if uptime is not None and previous_uptime is not None and uptime < previous_uptime:
        # The device rebooted. That is an event, not corrupt data, so nothing is
        # discarded - it is only counted.
        flags.append(FLAG_UPTIME_REGRESSED)
    return tuple(flags)


def validate_payload(
    payload: bytes,
    received_at: datetime,
    received_mono: float,
    previous_uptime: float | None = None,
) -> ValidatedMessage:
    try:
        decoded = json.loads(payload)
    except (ValueError, RecursionError) as exc:
        # JSONDecodeError and UnicodeDecodeError are both ValueError subclasses.
        # The base class also covers an integer literal over Python's 4300-digit
        # parsing limit (a plain ValueError), and RecursionError covers nesting
        # deeper than the C decoder will recurse. Uncaught, either would escape
        # to ingest's catch-all, which counts handler_errors and leaves the
        # message out of every data-quality figure.
        raise PayloadError(REASON_UNDECODABLE) from exc
    # `[1,2,3]`, `null`, `"hi"` and `42` are all valid JSON that json.loads
    # accepts, and .get() on them raises AttributeError. An undecodable counter
    # alone would miss them entirely.
    if not isinstance(decoded, dict):
        raise PayloadError(REASON_WRONG_SHAPE)

    readings: dict[str, float] = {}
    rejections: list[Rejection] = []
    for field in PLAUSIBLE_RANGES:
        reason = _reason_for(field, decoded)
        if reason is None:
            readings[field] = float(decoded[field])
        else:
            # A missing key has no raw value; repr(None) would be indistinguishable
            # from a genuine JSON null in recent_rejections.
            raw = "" if reason == REJECT_MISSING else repr(decoded[field])[:MAX_RAW_REPR]
            rejections.append(Rejection(field=field, reason=reason, raw=raw))

    timestamp, timestamp_source = parse_timestamp(decoded.get("timestamp"), received_at)

    sequence = decoded.get("sequence")
    if type(sequence) is not int:
        sequence = None
    device_id = decoded.get("device_id")
    if not isinstance(device_id, str):
        device_id = None

    return ValidatedMessage(
        readings=readings,
        rejections=tuple(rejections),
        timestamp=timestamp,
        timestamp_source=timestamp_source,
        received_mono=received_mono,
        sequence=sequence,
        device_id=device_id,
        consistency_flags=_consistency_flags(readings, previous_uptime),
    )
