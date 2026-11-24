"""Which device a message belongs to.

The device is named by the topic, not by the payload. The topic is known before
a single byte of the payload is decoded, so a truncated, bit-flipped or
array-wrapped payload still lands on the device that sent it. The payload's own
device_id is only ever compared with the topic's, never trusted over it.
"""
from __future__ import annotations

import re

# Starts with a letter or digit, so "." and ".." - names HTTP tooling treats as
# path segments - can never be device ids. 64 characters at most. Case-sensitive:
# "Gateway-01" and "gateway-01" are two devices.
DEVICE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"
DEFAULT_TOPIC_FILTER = "devices/+/telemetry"
DEFAULT_MAX_DEVICES = 32

_DEVICE_ID = re.compile(DEVICE_ID_PATTERN)


def is_valid_device_id(value: object) -> bool:
    # fullmatch, not match: `$` also matches just before a trailing "\n", so
    # re.match would accept "gateway-01\n" as a device id.
    return isinstance(value, str) and _DEVICE_ID.fullmatch(value) is not None


def check_topic_filter(topic_filter: str) -> None:
    """Refuse a filter the service could not route by: raise ValueError.

    Exactly one level must be "+", and that level is the device id. "#" would
    let a topic of any depth through with no single level to name the device,
    and an empty level is never what anybody meant to type. Called at startup,
    so a bad MQTT_TOPIC stops the service instead of silently routing nothing.
    """
    levels = topic_filter.split("/")
    if "" in levels:
        raise ValueError(f"MQTT_TOPIC={topic_filter!r} has an empty level")
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
