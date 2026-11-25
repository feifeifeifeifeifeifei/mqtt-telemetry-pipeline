"""Which device a message belongs to.

The device is named by the topic, not by the payload. The topic is known before
a single byte of the payload is decoded, so a truncated, bit-flipped or
array-wrapped payload still lands on the device that sent it. The payload's own
device_id is only ever compared with the topic's, never trusted over it.

DeviceRegistry gives each device a TelemetryStore of its own, and FleetCounters
holds the counts that belong to no device.
"""
from __future__ import annotations

import os
import re
import threading
from collections.abc import Mapping

from telemetry.store import TelemetryStore, _configured_history_window

# Starts with a letter or digit, so "." and ".." - names HTTP tooling treats as
# path segments - can never be device ids. 64 characters at most. Case-sensitive:
# "Gateway-01" and "gateway-01" are two devices.
DEVICE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"
DEFAULT_TOPIC_FILTER = "devices/+/telemetry"
DEFAULT_MAX_DEVICES = 32
# At roughly 1 MB of retained window per device, this is about 1 GB - far past
# anything this service is for, and still a number rather than no number.
MAX_DEVICES_CEILING = 1024

_DEVICE_ID = re.compile(DEVICE_ID_PATTERN)


def is_valid_device_id(value: object) -> bool:
    # fullmatch, not match: `$` also matches just before a trailing "\n", so
    # re.match would accept "gateway-01\n" as a device id.
    return isinstance(value, str) and _DEVICE_ID.fullmatch(value) is not None


def check_topic_filter(topic_filter: str) -> None:
    """Refuse a filter the service could not route by: raise ValueError.

    Exactly one level must be "+", and that level is the device id. "#" would
    let a topic of any depth through with no single level to name the device,
    and an empty level is never what anybody meant to type. A shared
    subscription ("$share/<group>/..." or "$queue/...") is refused too: the
    broker delivers its messages under their own topic, without the prefix, so
    not one would fit the filter. Called at startup, so a bad MQTT_TOPIC stops
    the service instead of silently routing nothing.
    """
    levels = topic_filter.split("/")
    if "" in levels:
        raise ValueError(f"MQTT_TOPIC={topic_filter!r} has an empty level")
    if levels[0] in ("$share", "$queue"):
        # The whole prefix the broker strips: "$share/<group>/" or "$queue/".
        prefix = "/".join(levels[: 2 if levels[0] == "$share" else 1]) + "/"
        raise ValueError(
            f"MQTT_TOPIC={topic_filter!r} is a shared subscription; messages arrive under "
            f"their own topic, without the {prefix!r} prefix, so drop the prefix"
        )
    if "#" in topic_filter:
        raise ValueError(f"MQTT_TOPIC={topic_filter!r} uses '#'; use one '+' level for the device id")
    # ["+"] and nothing else: no "+" level at all, two of them, and a "+" glued
    # to other characters ("a+", which MQTT itself rejects) all fail here.
    if [level for level in levels if "+" in level] != ["+"]:
        raise ValueError(f"MQTT_TOPIC={topic_filter!r} must have exactly one level that is just '+'")


def device_id_from_topic(topic_filter: str, topic: str) -> str | None:
    """The device a topic belongs to, or None when the topic names no device.

    topic_filter must already have passed check_topic_filter. The same number of
    levels, every fixed level equal character for character, and the "+" level
    a valid device id - anything else is None, never a guess.
    """
    wanted = topic_filter.split("/")
    actual = topic.split("/")
    if len(actual) != len(wanted):
        return None
    device_id = None
    for want, got in zip(wanted, actual):
        if want == "+":
            device_id = got
        elif want != got:
            return None
    return device_id if is_valid_device_id(device_id) else None


class DeviceRegistry:
    """Device id -> that device's own TelemetryStore, at most max_devices of them.

    The lock guards the dict and nothing else. Nothing here takes a store's
    lock or the FleetCounters lock while holding it, and no store method calls
    back into the registry, so no two locks are ever taken in opposite orders.
    Building a TelemetryStore under the lock is safe: its constructor only
    creates a lock, it never acquires one.

    There is no eviction. The broker is anonymous, so anyone can invent device
    names; the cap is what bounds the memory that costs.
    """

    def __init__(self, max_devices: int = DEFAULT_MAX_DEVICES, history_window: int | None = None) -> None:
        if not 1 <= max_devices <= MAX_DEVICES_CEILING:
            # An upper bound because the cap is the memory bound: this class's
            # docstring says the cap is what bounds what an anonymous broker can
            # cost, and MAX_DEVICES=1000000 would make that sentence false
            # without anything saying so. A typed extra zero is the realistic way
            # that happens.
            raise ValueError(f"MAX_DEVICES={max_devices} must be between 1 and {MAX_DEVICES_CEILING}")
        self._max_devices = max_devices
        # Read once, here: every device's window is the same size, whatever
        # happens to the environment after startup.
        self._history_window = _configured_history_window() if history_window is None else history_window
        self._lock = threading.Lock()
        self._stores: dict[str, TelemetryStore] = {}

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> DeviceRegistry:
        source = os.environ if env is None else env
        # `or`: compose turns an unset variable into an empty string.
        raw = source.get("MAX_DEVICES", "").strip() or str(DEFAULT_MAX_DEVICES)
        try:
            max_devices = int(raw)
        except ValueError:
            raise ValueError(f"MAX_DEVICES={raw!r} is not a whole number") from None
        return cls(max_devices=max_devices, history_window=_configured_history_window(source))

    @property
    def max_devices(self) -> int:
        return self._max_devices

    @property
    def history_window(self) -> int:
        return self._history_window

    def get_or_create(self, device_id: str) -> TelemetryStore | None:
        """The device's store, created on first sight; None once the cap is reached.

        Look-up, cap check and insert happen under one acquisition, so two
        threads that see the same new device cannot both create it, and racing
        new devices cannot overshoot the cap. device_id is trusted to be valid:
        it comes from device_id_from_topic.
        """
        with self._lock:
            store = self._stores.get(device_id)
            if store is None:
                if len(self._stores) >= self._max_devices:
                    return None
                store = self._stores[device_id] = TelemetryStore(history_window=self._history_window)
            return store

    def get(self, device_id: str) -> TelemetryStore | None:
        """Look-up only. The HTTP side uses this, so a request can never create a device."""
        with self._lock:
            return self._stores.get(device_id)

    def snapshot(self) -> list[tuple[str, TelemetryStore]]:
        """Every (device_id, store) pair, sorted by id, in a list the caller owns."""
        with self._lock:
            return sorted(self._stores.items())


class FleetCounters:
    """Counts that belong to no device: every message, and the ones no store took.

    Its own lock, never held together with the registry lock or a store lock.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._received = 0
        self._invalid_topic = 0
        self._over_device_limit = 0

    def record_received(self) -> None:
        with self._lock:
            self._received += 1

    def record_invalid_topic(self) -> None:
        with self._lock:
            self._invalid_topic += 1

    def record_over_device_limit(self) -> bool:
        """Count one message refused by the cap. True only for the very first,
        so the caller logs one WARNING rather than one per message."""
        with self._lock:
            self._over_device_limit += 1
            return self._over_device_limit == 1

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "messages_received": self._received,
                "messages_invalid_topic": self._invalid_topic,
                "messages_over_device_limit": self._over_device_limit,
            }
