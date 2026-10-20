"""What a healthy gateway reports at a given tick: a keyframe story plus a little noise.

Pure functions only - no clock, no network, no faults. The same tick and the
same random generator always give the same reading, which is what lets the
README quote exact values and the tests pin them.

Keyframes count messages, not seconds, because the service's anomaly rules
count consecutive messages. Written that way, the slope-versus-noise margins
below hold at any SIM_RATE_HZ: changing the rate only fast-forwards or slows
the story, and uptime is converted back to real seconds.
"""
from __future__ import annotations

import json
import random
from datetime import datetime

CYCLE_TICKS = 240      # the story replays every 240 messages: 4 minutes at 1 Hz
REBOOT_TICK = 180      # the firmware update installs and the gateway restarts
MEMORY_TOTAL_MB = 4096

# (tick, value) points joined by straight lines. Every track ends on the value
# it starts with, so the loop has no jump. Wherever a track crosses an alert
# threshold, its change per tick is larger than twice its noise amplitude, so
# consecutive readings move strictly one way and the threshold is crossed once;
# noise larger than the slope would split one event into several.
KEYFRAMES: dict[str, list[tuple[int, float]]] = {
    "cpu_usage_percent": [(0, 15), (30, 15), (36, 98), (110, 98), (113, 20),
                          (210, 16), (211, 97), (213, 97), (214, 16), (240, 15)],
    "cpu_temperature_c": [(0, 50), (30, 50), (60, 91), (110, 91), (140, 55), (240, 50)],
    "gpu_temperature_c": [(0, 43), (35, 43), (65, 79), (110, 79), (140, 47), (240, 43)],
    "memory_used_percent": [(0, 45), (30, 45), (55, 94), (110, 94), (115, 50), (240, 45)],
    "network_rx_kbps": [(0, 800), (30, 800), (36, 16000), (110, 16000), (113, 900),
                        (155, 900), (160, 6000), (178, 6000), (180, 800), (240, 800)],
    "network_tx_kbps": [(0, 150), (30, 150), (36, 3200), (110, 3200), (113, 160), (240, 150)],
    "disk_usage_percent": [(0, 88.5), (150, 91.5), (155, 72.0), (240, 88.5)],
    "battery_percent": [(0, 36.0), (120, 27.0), (190, 39.75), (240, 36.0)],
}

# Uniform noise amplitude, +/- A. Disk and battery get none: their rules fire on
# a single sample and they move by hundredths of a point per tick, so any noise
# would switch the alert on and off every second near the threshold.
NOISE: dict[str, float] = {
    "cpu_usage_percent": 1.5,
    "cpu_temperature_c": 0.4,
    "gpu_temperature_c": 0.4,
    "memory_used_percent": 0.4,
    "network_rx_kbps": 150,
    "network_tx_kbps": 40,
    "disk_usage_percent": 0,
    "battery_percent": 0,
}

# Story beats by position in the cycle; also the narration in the logs. Disk and
# battery are time-compressed: draining 0.075% a second takes hours on real
# hardware.
STORY: dict[int, str] = {
    0: "idle: background load only",
    30: "video-analytics batch starts: CPU, memory and network climb",
    110: "batch done: load drops, fans cool the chips",
    120: "technician plugs in the charger",
    150: "operator deletes old video clips remotely: disk drops to 72%",
    155: "downloading firmware update",
    180: "rebooting to install firmware (uptime restarts at 0)",
    190: "charger unplugged",
    210: "backup job: CPU bursts for 3 samples, too short to alert (rule needs 5)",
}


def interpolate(points: list[tuple[int, float]], tick: float) -> float:
    """The value on the straight line between the two keyframes around `tick`."""
    for (t0, v0), (t1, v1) in zip(points, points[1:]):
        if t0 <= tick <= t1:
            return v0 + (v1 - v0) * (tick - t0) / (t1 - t0)
    raise ValueError(f"tick {tick} is outside the keyframes")


def uptime_seconds(tick: int, rate_hz: float) -> int:
    """Seconds since the last boot: power-on at tick 0, a reboot at REBOOT_TICK of every cycle.

    Divided by the rate so uptime stays in real seconds whatever SIM_RATE_HZ is.
    """
    since_boot = tick if tick < REBOOT_TICK else (tick - REBOOT_TICK) % CYCLE_TICKS
    return int(since_boot / rate_hz)


def reading(tick: int, rng: random.Random, rate_hz: float) -> dict[str, float | int]:
    """The eleven values a healthy gateway reports at `tick`."""
    position = tick % CYCLE_TICKS
    values: dict[str, float | int] = {}
    for field, points in KEYFRAMES.items():
        # Only rng.random(): it is the one method Python promises will keep
        # producing the same sequence for the same seed across versions.
        noise = (rng.random() * 2 - 1) * NOISE[field]
        values[field] = round(interpolate(points, position) + noise, 2)
    values["memory_total_mb"] = MEMORY_TOTAL_MB
    # Derived from the percentage, so the service's cross-field memory check
    # passes on every clean message.
    values["memory_used_mb"] = round(MEMORY_TOTAL_MB * values["memory_used_percent"] / 100, 1)
    values["uptime_seconds"] = uptime_seconds(tick, rate_hz)
    return values


def build_message(tick: int, now: datetime, device_id: str, values: dict) -> dict:
    """The wire message. `sequence` is the tick and never resets; the service does not use it."""
    return {"device_id": device_id, "sequence": tick, "timestamp": now.isoformat(), **values}


def encode(value: object) -> bytes:
    """json.dumps with its defaults, on purpose.

    Keys keep insertion order, floats keep their `.0`, and NaN is written as the
    bare token NaN, which one of the faults relies on.
    """
    return json.dumps(value).encode()
