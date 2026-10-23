"""Simulator settings, read once from the environment and checked at startup.

A bad value fails here, in the first second, instead of as a crash at frame
300. The MQTT variable names are the service's own, so one `.env` serves both
containers; the simulator's own knobs carry a SIM_ prefix.
"""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_TOPIC = "devices/gateway-01/telemetry"
DEFAULT_DEVICE_ID = "gateway-01"


def _number(name: str, raw: str, default: str, parse=int):
    """Parse one setting, or fail naming the variable.

    A bare int()/float() reports "invalid literal for int() with base 10: ''",
    which names neither the variable nor what it would have accepted - and an
    empty string is what compose sends for a variable nobody set.
    """
    try:
        return parse(raw.strip() or default)
    except ValueError:
        wanted = "a whole number" if parse is int else "a number"
        raise ValueError(f"{name}={raw!r} is not {wanted}") from None


def _port(raw: str) -> int:
    """MQTT_PORT, with the same message the service gives.

    The simulator used to take whatever int() accepted, so a port of 0 or 70000
    started it and it then span for ever without connecting - while the service
    rejected the same value by name. Two programs reading one `.env` should not
    disagree about what it says.
    """
    port = _number("MQTT_PORT", raw, "1883")
    if not 1 <= port <= 65535:
        raise ValueError(f"MQTT_PORT={port} is outside 1-65535")
    return port


@dataclass(frozen=True)
class Config:
    host: str
    port: int
    username: str | None
    password: str | None
    topic: str
    device_id: str
    rate_hz: float
    seed: int
    fault_every: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        # `env` is injectable so tests can pass a dict instead of monkeypatching
        # the process environment - the same shape as the service's BrokerConfig.
        source = os.environ if env is None else env
        config = cls(
            # Same rules as the service's BrokerConfig: compose turns an unset
            # variable into an empty string, so "" has to mean "unset" rather
            # than a host named "" or int("") raising at startup.
            host=source.get("MQTT_HOST", "").strip() or "localhost",
            port=_port(source.get("MQTT_PORT", "")),
            # `or None`: compose hands an unset variable over as "", and an
            # empty username has to mean anonymous, not a login named "".
            username=source.get("MQTT_USERNAME") or None,
            password=source.get("MQTT_PASSWORD") or None,
            # Empty means the default too: paho refuses to publish to "".
            topic=source.get("MQTT_TOPIC") or DEFAULT_TOPIC,
            device_id=source.get("SIM_DEVICE_ID", DEFAULT_DEVICE_ID),
            rate_hz=_number("SIM_RATE_HZ", source.get("SIM_RATE_HZ", ""), "1", float),
            seed=_number("SIM_SEED", source.get("SIM_SEED", ""), "7"),
            fault_every=_number("SIM_FAULT_EVERY", source.get("SIM_FAULT_EVERY", ""), "10"),
        )
        if config.password and not config.username:
            # The publisher logs in only when a username is set, so a lone
            # password would be dropped without a word.
            raise ValueError("MQTT_PASSWORD is set but MQTT_USERNAME is not")
        if not config.device_id:
            raise ValueError("SIM_DEVICE_ID must not be empty")
        if not 0.1 <= config.rate_hz <= 10:
            # 0.1 Hz is one message every 10 s, still inside the service's 30 s
            # staleness limit. The rate only fast-forwards the story, and at
            # 10 Hz a whole cycle already passes in 24 s. NaN fails here too.
            raise ValueError("SIM_RATE_HZ must be between 0.1 and 10")
        if config.fault_every < 0 or config.fault_every == 1:
            # 1 would put a catalogue entry in every frame, leaving no clean
            # baseline to compare against.
            raise ValueError("SIM_FAULT_EVERY must be 0 (off) or at least 2")
        return config
