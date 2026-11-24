"""Tests for telemetry.fleet.

Not in mutmut's test selection on purpose: mutmut only mutates validation.py,
and these tests would only slow every mutant down.
"""
from __future__ import annotations

import pytest

from telemetry import fleet

# ------------------------------------------------------------- device ids


def test_the_device_id_rule_is_the_documented_one():
    # The literal, not a derived value: the API's path pattern and the
    # simulator's copy are both checked against this string.
    assert fleet.DEVICE_ID_PATTERN == r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"


def test_the_fleet_defaults_are_the_documented_ones():
    assert fleet.DEFAULT_TOPIC_FILTER == "devices/+/telemetry"
    assert fleet.DEFAULT_MAX_DEVICES == 32


@pytest.mark.parametrize(
    "value",
    [
        "gateway-01", "a", "Z", "7", "a" * 64, "Gateway-01", "a.b_c-d", "a..b", "1.2.3",
    ],
    ids=["typical", "one-letter", "upper", "digit", "64-chars", "mixed-case",
         "inner-punctuation", "inner-dots", "dotted-version"],
)
def test_valid_device_ids(value):
    assert fleet.is_valid_device_id(value) is True


@pytest.mark.parametrize(
    "value",
    [
        "", "a" * 65, ".hidden", ".", "..", "-a", "_a",
        "a+b", "+", "a#b", "#", "a/b", "/", "a b",
        "gateway-01\n",
        "gatewäy", "设备-01", "١٢٣",
    ],
    ids=["empty", "65-chars", "leading-dot", "dot", "dot-dot", "leading-dash",
         "leading-underscore", "plus-inside", "plus", "hash-inside", "hash",
         "slash-inside", "slash", "space",
         "trailing-newline",
         "latin-umlaut", "cjk", "arabic-indic-digits"],
)
def test_invalid_device_ids(value):
    # trailing-newline: `$` also matches just before a final "\n", so re.match
    # with this pattern would accept it. fullmatch is what keeps it out.
    # The non-ASCII cases: [A-Za-z0-9] is ASCII-only, unlike \w and \d.
    assert fleet.is_valid_device_id(value) is False


@pytest.mark.parametrize("value", [None, 17, b"gateway-01", ["gateway-01"]],
                         ids=["none", "int", "bytes", "list"])
def test_a_non_string_is_never_a_device_id(value):
    assert fleet.is_valid_device_id(value) is False


# ---------------------------------------------------------- topic filters


@pytest.mark.parametrize(
    "topic_filter",
    ["devices/+/telemetry", "+", "devices/+", "+/telemetry", "fleet/site-a/+/telemetry"],
)
def test_valid_topic_filters(topic_filter):
    assert fleet.check_topic_filter(topic_filter) is None


@pytest.mark.parametrize(
    "topic_filter",
    [
        "",
        "devices/gateway-01/telemetry",
        "devices/+/+/telemetry",
        "devices/#",
        "devices/+/#",
        "devices/+/tele#metry",
        "devices//telemetry",
        "devices/+//telemetry",
        "/devices/+/telemetry",
        "devices/+/telemetry/",
        "devices/a+/telemetry",
        "devices/+a/telemetry",
        "devices/+/telemetry+",
    ],
    ids=["empty", "no-plus", "two-plus-levels", "hash-level", "plus-and-hash",
         "hash-inside-a-level", "empty-level-no-plus", "empty-level-with-plus",
         "leading-slash", "trailing-slash", "plus-glued-before", "plus-glued-after",
         "whole-plus-and-glued-plus"],
)
def test_invalid_topic_filters_raise_naming_the_variable(topic_filter):
    # Each case trips one rule the others would miss: plus-and-hash has exactly
    # one "+" level, empty-level-with-plus too, and plus-glued-before has a "+"
    # but no level that is only "+".
    with pytest.raises(ValueError, match="MQTT_TOPIC"):
        fleet.check_topic_filter(topic_filter)


# ---------------------------------------------------------- topic -> device


@pytest.mark.parametrize(
    "topic_filter, topic, expected",
    [
        ("devices/+/telemetry", "devices/gateway-01/telemetry", "gateway-01"),
        ("devices/+/telemetry", "devices/Gateway-01/telemetry", "Gateway-01"),
        ("devices/+/telemetry", "devices/" + "a" * 64 + "/telemetry", "a" * 64),
        ("+", "gateway-01", "gateway-01"),
        ("devices/+", "devices/gw", "gw"),
        ("fleet/site-a/+/telemetry", "fleet/site-a/gw/telemetry", "gw"),
    ],
    ids=["default-filter", "case-kept", "64-chars", "bare-plus", "plus-last", "plus-third"],
)
def test_the_plus_level_names_the_device(topic_filter, topic, expected):
    assert fleet.device_id_from_topic(topic_filter, topic) == expected


@pytest.mark.parametrize(
    "topic",
    [
        "devices/gateway-01/status",
        "fleet/gateway-01/telemetry",
        "devices/a/b/telemetry",
        "devices/telemetry",
        "devices/gateway-01/telemetry/",
        "/devices/gateway-01/telemetry",
        "devices//telemetry",
        "devices/.hidden/telemetry",
        "devices/+/telemetry",
        "devices/" + "a" * 65 + "/telemetry",
        "Devices/gateway-01/telemetry",
        "devices/gateway-01/Telemetry",
    ],
    ids=["wrong-suffix", "wrong-prefix", "extra-level", "missing-level", "trailing-slash",
         "leading-slash", "empty-level", "invalid-id", "literal-plus", "65-char-id",
         "prefix-case", "suffix-case"],
)
def test_a_topic_that_does_not_fit_the_filter_names_no_device(topic):
    assert fleet.device_id_from_topic("devices/+/telemetry", topic) is None


def test_the_same_id_in_another_case_is_another_device():
    upper = fleet.device_id_from_topic("devices/+/telemetry", "devices/GW/telemetry")
    lower = fleet.device_id_from_topic("devices/+/telemetry", "devices/gw/telemetry")
    assert (upper, lower) == ("GW", "gw")
