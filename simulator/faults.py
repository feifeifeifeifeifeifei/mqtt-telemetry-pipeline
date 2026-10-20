"""How real messages go wrong, and the verdict the service must reach for each.

Every catalogue entry names a fault, gives its real-world cause, corrupts a
message, and states the exact verdict the service's validator must produce.
The contract tests compare that verdict string for string, so this table is
also the test table. Verdicts read:

- `<field>:<reason>`   the field is rejected
- `payload:<reason>`   the whole message cannot be decoded or has the wrong shape
- `timestamp:<source>` the timestamp is replaced by the receipt time
- `flag:<flag>`        a cross-field check fires
- `clean`              nothing is wrong
"""
from __future__ import annotations

import email.utils
import math
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from simulator.device import encode

RTC_EPOCH = datetime(2000, 1, 1)    # where a real-time clock restarts when its backup cell dies
UTC_PLUS_8 = timezone(timedelta(hours=8))
# A 32-bit byte counter that wrapped below zero, expressed in kilobits.
COUNTER_WRAP_KBPS = -round(2**32 * 8 / 1000, 1)


@dataclass(frozen=True)
class Fault:
    """One catalogue entry. `apply` receives a private copy of the message and may change it freely."""

    name: str
    story: str      # the real-world cause in one sentence, for the logs and the README
    expect: str     # the service's verdict, compared verbatim by the contract tests
    apply: Callable[[dict, datetime, random.Random], bytes]


def _sensor_timeout(message: dict, now: datetime, rng: random.Random) -> bytes:
    del message["gpu_temperature_c"]
    return encode(message)


def _truncated(message: dict, now: datetime, rng: random.Random) -> bytes:
    # Always undecodable: the closing brace is the last byte, so every proper
    # prefix of a JSON object lacks it.
    whole = encode(message)
    return whole[: int(len(whole) * (0.2 + 0.6 * rng.random()))]


def _null_reading(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["battery_percent"] = None
    return encode(message)


def _rtc_reset(message: dict, now: datetime, rng: random.Random) -> bytes:
    # No time zone, as an RTC chip has none; this also takes the validator down
    # its "naive means UTC" branch before the 26-year skew is judged.
    clock = RTC_EPOCH + timedelta(seconds=message["uptime_seconds"])
    message["timestamp"] = clock.isoformat()
    return encode(message)


def _number_as_text(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["cpu_usage_percent"] = str(message["cpu_usage_percent"])
    return encode(message)


def _adc_saturated(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["cpu_temperature_c"] = 255.0
    return encode(message)


def _batched_array(message: dict, now: datetime, rng: random.Random) -> bytes:
    return encode([message])


def _nan_reading(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["gpu_temperature_c"] = math.nan     # json.dumps writes the bare token NaN
    return encode(message)


def _epoch_millis(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["timestamp"] = int(now.timestamp() * 1000)
    return encode(message)


def _stale_memory_counter(message: dict, now: datetime, rng: random.Random) -> bytes:
    # Halving `used` moves it at least 22 points off the percentage, far outside
    # the service's 1.5-point tolerance.
    message["memory_used_mb"] = round(message["memory_used_mb"] / 2, 1)
    return encode(message)


def _boolean_reading(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["memory_used_percent"] = False      # bool subclasses int in Python
    return encode(message)


def _bit_flip(message: dict, now: datetime, rng: random.Random) -> bytes:
    # 0xFF never occurs in valid UTF-8. Starting after byte 8 leaves the first
    # four bytes alone, which are all json looks at to guess the encoding.
    data = bytearray(encode(message))
    for _ in range(3):
        data[8 + int(rng.random() * (len(data) - 8))] = 0xFF
    return bytes(data)


def _huge_counter(message: dict, now: datetime, rng: random.Random) -> bytes:
    # 401 digits: too big for a float, well under Python's 4300-digit limit
    # on parsing integers, so it reaches the validator intact.
    message["uptime_seconds"] = 10**400
    return encode(message)


def _counter_wrap(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["network_rx_kbps"] = COUNTER_WRAP_KBPS
    return encode(message)


def _http_date(message: dict, now: datetime, rng: random.Random) -> bytes:
    # usegmt=True accepts only UTC, hence the conversion; format_datetime is
    # also independent of the locale, unlike strftime's day and month names.
    message["timestamp"] = email.utils.format_datetime(now.astimezone(timezone.utc), usegmt=True)
    return encode(message)


def _firmware_v2(message: dict, now: datetime, rng: random.Random) -> bytes:
    message["fan_rpm"] = 2000 + int(rng.random() * 3000)
    message["timestamp"] = now.astimezone(UTC_PLUS_8).isoformat()
    return encode(message)


# The order is the rotation order.
CATALOGUE: tuple[Fault, ...] = (
    Fault("sensor-timeout", "GPU sensor did not answer",
          "gpu_temperature_c:missing", _sensor_timeout),
    Fault("truncated", "radio dropped the tail of the packet",
          "payload:undecodable", _truncated),
    Fault("null-reading", "fuel gauge timed out on the I2C bus",
          "battery_percent:null", _null_reading),
    Fault("rtc-reset", "RTC backup cell is flat, the clock restarted at 2000-01-01 with no time zone",
          "timestamp:receipt-implausible", _rtc_reset),
    Fault("number-as-text", "firmware put quotes around a number",
          "cpu_usage_percent:not-a-number", _number_as_text),
    Fault("adc-saturated", "thermistor wire broke, 8-bit ADC reads full scale",
          "cpu_temperature_c:out-of-range", _adc_saturated),
    Fault("batched-array", "a single message was wrapped in a batch array",
          "payload:wrong-shape", _batched_array),
    Fault("nan-reading", "driver divided by zero",
          "gpu_temperature_c:not-finite", _nan_reading),
    Fault("epoch-millis", "timestamp sent as JavaScript-style epoch milliseconds",
          "timestamp:receipt-unparseable", _epoch_millis),
    Fault("stale-memory-counter", "used memory was read before the cache was reclaimed",
          "flag:memory-inconsistent", _stale_memory_counter),
    Fault("boolean-reading", "a status bit was sent instead of a reading",
          "memory_used_percent:not-a-number", _boolean_reading),
    Fault("bit-flip", "line noise corrupted three bytes",
          "payload:undecodable", _bit_flip),
    Fault("huge-counter", "the memory holding the uptime counter was corrupted",
          "uptime_seconds:not-finite", _huge_counter),
    Fault("counter-wrap", "32-bit byte counter wrapped around",
          "network_rx_kbps:out-of-range", _counter_wrap),
    Fault("http-date", "firmware used the HTTP date format",
          "timestamp:receipt-unparseable", _http_date),
    Fault("firmware-v2", "new firmware adds fan_rpm and reports local time at +08:00",
          "clean", _firmware_v2),
)


def fault_for(tick: int, seed: int, every: int) -> Fault | None:
    """The catalogue entry for this tick, or None for a clean frame.

    The stream is cut into blocks of `every` frames. Each block holds exactly
    one entry, at a position the seed picks, and entries rotate in catalogue
    order: the rate is exactly 1/every, and all 16 are seen within 16 blocks,
    which random injection could not promise.
    """
    if every == 0:
        return None
    block = tick // every
    slot = int(random.Random(f"{seed}:{block}:slot").random() * every)
    if tick % every != slot:
        return None
    return CATALOGUE[block % len(CATALOGUE)]
