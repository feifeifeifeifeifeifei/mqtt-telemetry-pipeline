from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from telemetry.validation import (
    FLAG_MEMORY_INCONSISTENT,
    MAX_CLOCK_SKEW_SECONDS,
    MAX_RAW_REPR,
    MEMORY_TOLERANCE_POINTS,
    FLAG_UPTIME_REGRESSED,
    PLAUSIBLE_RANGES,
    PayloadError,
    REJECT_MISSING,
    REJECT_NOT_A_NUMBER,
    REJECT_NOT_FINITE,
    REJECT_NULL,
    REJECT_OUT_OF_RANGE,
    SOURCE_IMPLAUSIBLE,
    SOURCE_PAYLOAD,
    SOURCE_UNPARSEABLE,
    isoformat_z,
    validate_payload,
)

RECEIVED_AT = datetime(2026, 10, 19, 8, 0, 57, tzinfo=timezone.utc)
RECEIVED_MONO = 1000.0

# A healthy frame as the device simulator emits it (seed 7, tick 57).
GOOD = {
    "device_id": "gateway-01",
    "sequence": 57,
    "timestamp": "2026-10-19T08:00:57+00:00",
    "cpu_usage_percent": 97.64,
    "cpu_temperature_c": 86.66,
    "gpu_temperature_c": 69.04,
    "memory_used_percent": 93.91,
    "network_rx_kbps": 16081.4,
    "network_tx_kbps": 3196.39,
    "disk_usage_percent": 89.64,
    "battery_percent": 31.73,
    "memory_total_mb": 4096,
    "memory_used_mb": 3846.6,
    "uptime_seconds": 57,
}

ABSENT = object()


def payload(**overrides) -> bytes:
    body = dict(GOOD)
    for key, value in overrides.items():
        if value is ABSENT:
            body.pop(key)
        else:
            body[key] = value
    return json.dumps(body).encode()


def validate(raw: bytes, **kwargs):
    return validate_payload(raw, RECEIVED_AT, RECEIVED_MONO, **kwargs)


def test_healthy_message_produces_every_reading():
    result = validate(payload())
    assert result.rejections == ()
    assert set(result.readings) == set(PLAUSIBLE_RANGES)
    assert result.readings["cpu_temperature_c"] == 86.66
    assert result.sequence == 57
    assert result.device_id == "gateway-01"
    assert result.timestamp_source == SOURCE_PAYLOAD
    assert result.received_mono == RECEIVED_MONO
    assert result.consistency_flags == ()


def test_accepted_values_are_floats_even_when_the_payload_had_ints():
    result = validate(payload())
    assert isinstance(result.readings["memory_total_mb"], float)


def test_one_bad_field_does_not_discard_the_others():
    result = validate(payload(cpu_usage_percent="97.64"))
    assert "cpu_usage_percent" not in result.readings
    assert len(result.readings) == len(PLAUSIBLE_RANGES) - 1
    assert result.readings["cpu_temperature_c"] == 86.66
    assert [(r.field, r.reason) for r in result.rejections] == [
        ("cpu_usage_percent", REJECT_NOT_A_NUMBER)
    ]
    assert result.rejections[0].raw == "'97.64'"


@pytest.mark.parametrize(
    "field, value, reason",
    [
        ("cpu_temperature_c", ABSENT, REJECT_MISSING),
        ("cpu_temperature_c", None, REJECT_NULL),
        ("cpu_temperature_c", "", REJECT_NOT_A_NUMBER),
        ("cpu_temperature_c", 255.0, REJECT_OUT_OF_RANGE),
        ("gpu_temperature_c", -273.15, REJECT_OUT_OF_RANGE),
        ("cpu_usage_percent", 100.5, REJECT_OUT_OF_RANGE),
        ("battery_percent", 101.0, REJECT_OUT_OF_RANGE),
        ("disk_usage_percent", -0.5, REJECT_OUT_OF_RANGE),
        ("network_tx_kbps", "0x1F", REJECT_NOT_A_NUMBER),
        ("network_rx_kbps", "nan", REJECT_NOT_A_NUMBER),
    ],
)
def test_typical_fault_shapes_are_rejected_with_the_right_reason(field, value, reason):
    result = validate(payload(**{field: value}))
    assert field not in result.readings
    assert [(r.field, r.reason) for r in result.rejections] == [(field, reason)]


@pytest.mark.parametrize(
    "field, value",
    [("memory_used_percent", False), ("battery_percent", True), ("cpu_usage_percent", False)],
)
def test_boolean_is_rejected_even_though_it_is_an_int_subclass(field, value):
    # isinstance(False, int) is True, so a naive numeric check would accept this
    # and average it in as 0.0 or 1.0 with no error and no log line.
    result = validate(payload(**{field: value}))
    assert field not in result.readings
    assert result.rejections[0].reason == REJECT_NOT_A_NUMBER


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_numbers_are_rejected(literal):
    # Python's json accepts these non-standard literals by default, and NaN
    # compares False against everything, which breaks min/max silently.
    raw = json.dumps(GOOD).replace('"cpu_temperature_c": 86.66', f'"cpu_temperature_c": {literal}')
    result = validate(raw.encode())
    assert "cpu_temperature_c" not in result.readings
    assert result.rejections[0].reason == REJECT_NOT_FINITE


def test_integer_too_large_for_a_float_is_rejected_not_raised():
    # math.isfinite(10**400) raises OverflowError rather than returning False.
    raw = json.dumps(GOOD).replace('"network_rx_kbps": 16081.4', f'"network_rx_kbps": {10**400}')
    result = validate(raw.encode())
    assert "network_rx_kbps" not in result.readings
    assert result.rejections[0].reason == REJECT_NOT_FINITE


def test_a_value_just_above_the_ceiling_is_rejected():
    result = validate(payload(network_tx_kbps=10_000_000.5))
    assert "network_tx_kbps" not in result.readings
    assert result.rejections[0].reason == REJECT_OUT_OF_RANGE


def test_payload_error_carries_its_reason_as_the_message():
    # LOGGER.exception renders the exception, so the message has to say something.
    with pytest.raises(PayloadError) as excinfo:
        validate(b"{not json")
    assert str(excinfo.value) == "undecodable"


def test_truncated_payload_raises_undecodable():
    whole = json.dumps(GOOD)
    with pytest.raises(PayloadError) as excinfo:
        validate(whole[: len(whole) // 2].encode())
    assert excinfo.value.reason == "undecodable"


def test_invalid_utf8_raises_undecodable():
    with pytest.raises(PayloadError) as excinfo:
        validate(b"\xff\xfe not json")
    assert excinfo.value.reason == "undecodable"


def with_raw_value(field: str, literal: bytes) -> bytes:
    """A healthy payload whose `field` holds a JSON literal json.dumps cannot emit."""
    return payload(**{field: "__RAW__"}).replace(b'"__RAW__"', literal)


def test_an_integer_over_the_parser_digit_limit_raises_undecodable():
    # Since 3.11 int() refuses to parse more than 4300 digits. json.loads lets
    # that plain ValueError through - it is not a JSONDecodeError.
    with pytest.raises(PayloadError) as excinfo:
        validate(with_raw_value("uptime_seconds", b"9" * 4301))
    assert excinfo.value.reason == "undecodable"
    # `type() is`, not isinstance: JSONDecodeError subclasses ValueError, so
    # isinstance would also pass on a payload that is simply malformed.
    assert type(excinfo.value.__cause__) is ValueError


def test_json_nested_past_the_recursion_limit_raises_undecodable():
    # The C decoder recurses once per bracket and gives up with RecursionError.
    # 100_000 levels is far past the limit on 3.11 (the image) and on 3.14.
    deep = b"[" * 100_000 + b"]" * 100_000
    with pytest.raises(PayloadError) as excinfo:
        validate(with_raw_value("cpu_usage_percent", deep))
    assert excinfo.value.reason == "undecodable"
    assert isinstance(excinfo.value.__cause__, RecursionError)


@pytest.mark.parametrize("value", [10**400, int("9" * 4300)], ids=["401-digits", "4300-digits"])
def test_an_integer_the_parser_accepts_stays_a_field_level_rejection(value):
    # Up to 4300 digits the payload decodes, so the damage stays confined to one
    # field: math.isfinite raises OverflowError, which maps to not-finite.
    result = validate(payload(uptime_seconds=value))
    assert "uptime_seconds" not in result.readings
    assert [(r.field, r.reason) for r in result.rejections] == [
        ("uptime_seconds", REJECT_NOT_FINITE)
    ]


@pytest.mark.parametrize("raw", [b"[1,2,3]", b"null", b'"hello"', b"42", b"true"])
def test_valid_json_that_is_not_an_object_raises_wrong_shape(raw):
    # These parse fine, so an undecodable counter alone would miss them and
    # .get() on the result would raise AttributeError.
    with pytest.raises(PayloadError) as excinfo:
        validate(raw)
    assert excinfo.value.reason == "wrong-shape"


@pytest.mark.parametrize(
    "bad",
    [
        "", "Mon, 19 Oct 2026 08:02:20 GMT", "2026-02-30T08:00:00", "19/10/2026 08:00",
        "1792396857000", None, 17, True,
    ],
)
def test_unparseable_timestamp_falls_back_to_receipt_time(bad):
    result = validate(payload(timestamp=bad))
    assert result.timestamp == RECEIVED_AT
    assert result.timestamp_source == SOURCE_UNPARSEABLE


def test_well_formed_but_implausible_timestamp_falls_back():
    # A device clock that was set wrong sends valid ISO-8601 dates hours from the
    # truth. They pass every field-level check and are only visible against
    # receipt time.
    far = (RECEIVED_AT - timedelta(hours=3)).isoformat().replace("+00:00", "Z")
    result = validate(payload(timestamp=far))
    assert result.timestamp == RECEIVED_AT
    assert result.timestamp_source == SOURCE_IMPLAUSIBLE


def test_a_timestamp_just_inside_the_skew_window_is_kept():
    near = (RECEIVED_AT - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
    result = validate(payload(timestamp=near))
    assert result.timestamp_source == SOURCE_PAYLOAD
    assert result.timestamp == RECEIVED_AT - timedelta(seconds=5)


def test_naive_timestamp_is_treated_as_utc_and_does_not_raise():
    # Subtracting a naive datetime from an aware one raises TypeError.
    naive = RECEIVED_AT.replace(tzinfo=None).isoformat()
    result = validate(payload(timestamp=naive))
    assert result.timestamp_source == SOURCE_PAYLOAD
    assert result.timestamp.tzinfo is not None


@pytest.mark.parametrize("value", ["abc", True, 3.5, None])
def test_sequence_that_is_not_an_int_becomes_none(value):
    assert validate(payload(sequence=value)).sequence is None


@pytest.mark.parametrize("value", [17, None, ["a"]])
def test_device_id_that_is_not_a_string_becomes_none(value):
    assert validate(payload(device_id=value)).device_id is None


def test_memory_inconsistency_is_flagged_but_nothing_is_discarded():
    result = validate(payload(memory_used_mb=1000.0, memory_total_mb=4096, memory_used_percent=95.0))
    assert FLAG_MEMORY_INCONSISTENT in result.consistency_flags
    assert result.readings["memory_used_mb"] == 1000.0
    assert result.readings["memory_used_percent"] == 95.0


def test_memory_consistency_does_not_divide_by_zero():
    result = validate(payload(memory_total_mb=0))
    assert FLAG_MEMORY_INCONSISTENT not in result.consistency_flags


def test_uptime_regression_is_flagged():
    result = validate(payload(uptime_seconds=10), previous_uptime=500.0)
    assert FLAG_UPTIME_REGRESSED in result.consistency_flags
    assert result.readings["uptime_seconds"] == 10.0


def test_uptime_moving_forward_is_not_flagged():
    result = validate(payload(uptime_seconds=600), previous_uptime=500.0)
    assert result.consistency_flags == ()


# --------------------------------------------------------- contract surfaces
# These pin the values that cross the wire. Every test above compares against
# the imported constants, so without this the constants are only self-consistent
# and renaming one to "nil" would leave the whole suite green.


def test_reject_reasons_are_the_documented_wire_values():
    assert (
        REJECT_MISSING, REJECT_NULL, REJECT_NOT_A_NUMBER, REJECT_NOT_FINITE, REJECT_OUT_OF_RANGE
    ) == ("missing", "null", "not-a-number", "not-finite", "out-of-range")


def test_timestamp_sources_and_flags_are_the_documented_wire_values():
    assert (SOURCE_PAYLOAD, SOURCE_UNPARSEABLE, SOURCE_IMPLAUSIBLE) == (
        "payload", "receipt-unparseable", "receipt-implausible",
    )
    assert (FLAG_MEMORY_INCONSISTENT, FLAG_UPTIME_REGRESSED) == (
        "memory-inconsistent", "uptime-regressed",
    )


def test_plausible_ranges_match_the_documented_table():
    assert PLAUSIBLE_RANGES == {
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


@pytest.mark.parametrize(
    "field, value, accepted",
    [
        ("battery_percent", 0, True), ("battery_percent", 100, True),
        ("battery_percent", -0.1, False), ("battery_percent", 100.1, False),
        ("cpu_temperature_c", -20, True), ("cpu_temperature_c", 120, True),
        ("cpu_temperature_c", -20.1, False), ("cpu_temperature_c", 120.1, False),
    ],
)
def test_range_bounds_are_inclusive(field, value, accepted):
    result = validate(payload(**{field: value}))
    assert (field in result.readings) is accepted


# ------------------------------------------------------- threshold boundaries


def test_clock_skew_threshold_is_sixty_seconds():
    assert MAX_CLOCK_SKEW_SECONDS == 60.0


@pytest.mark.parametrize("offset, source", [(60.0, SOURCE_PAYLOAD), (60.001, SOURCE_IMPLAUSIBLE)])
def test_clock_skew_boundary_is_exact(offset, source):
    moment = (RECEIVED_AT - timedelta(seconds=offset)).isoformat().replace("+00:00", "Z")
    assert validate(payload(timestamp=moment)).timestamp_source == source


def test_a_future_dated_timestamp_is_implausible_too():
    # The skew check is symmetric: a clock running ahead is as implausible as
    # one running behind, and only the past was exercised above.
    ahead = (RECEIVED_AT + timedelta(hours=3)).isoformat().replace("+00:00", "Z")
    assert validate(payload(timestamp=ahead)).timestamp_source == SOURCE_IMPLAUSIBLE


def test_memory_tolerance_is_one_and_a_half_points():
    assert MEMORY_TOLERANCE_POINTS == 1.5


@pytest.mark.parametrize(
    "percent, flagged",
    [
        (51.0, False),    # 1.0 points away
        (51.5, False),    # exactly on the tolerance: the comparison is strict >
        (51.6, True),     # just past it
        (48.6, False),    # 1.4 points the other way - also pins the *100.0 scale
        (53.0, True),     # 3.0 points away
    ],
)
def test_memory_tolerance_boundary(percent, flagged):
    # used/total is exactly 50.0%, so `percent` is the whole discrepancy.
    result = validate(payload(memory_used_mb=2048.0, memory_total_mb=4096, memory_used_percent=percent))
    assert (FLAG_MEMORY_INCONSISTENT in result.consistency_flags) is flagged


def test_memory_consistency_still_runs_when_total_is_one_megabyte():
    # The guard is `total > 0`, a division-by-zero check - not a size threshold.
    result = validate(payload(memory_used_mb=1.0, memory_total_mb=1, memory_used_percent=50.0))
    assert FLAG_MEMORY_INCONSISTENT in result.consistency_flags


def test_identical_uptime_is_not_a_regression():
    # A re-sent message - a QoS 1 redelivery, a publisher retrying - carries an
    # identical uptime. A `<=` test would report each one as a device reboot.
    assert validate(payload(uptime_seconds=500), previous_uptime=500.0).consistency_flags == ()


# --------------------------------------------------------- raw value fidelity


def test_raw_rejection_value_is_truncated_to_sixty_characters():
    # The literal, not the constant: comparing against MAX_RAW_REPR would be
    # self-consistent under any change to it and would verify nothing.
    assert MAX_RAW_REPR == 60
    result = validate(payload(cpu_temperature_c="x" * 200))
    assert len(result.rejections[0].raw) == 60


def test_a_missing_field_reports_no_raw_value():
    # repr(None) would be indistinguishable from a genuine JSON null.
    missing = validate(payload(cpu_temperature_c=ABSENT)).rejections[0]
    null = validate(payload(cpu_temperature_c=None)).rejections[0]
    assert missing.raw == ""
    assert null.raw == "None"


# -------------------------------------------------------------- isoformat_z


def test_isoformat_z_renders_utc_with_a_z_suffix():
    assert isoformat_z(datetime(2026, 10, 19, 8, 0, 57, tzinfo=timezone.utc)) == "2026-10-19T08:00:57Z"


def test_isoformat_z_normalises_a_non_utc_offset():
    berlin = datetime(2026, 10, 19, 10, 0, 57, tzinfo=timezone(timedelta(hours=2)))
    assert isoformat_z(berlin) == "2026-10-19T08:00:57Z"


def test_an_accepted_non_utc_timestamp_is_stored_as_utc():
    local = (RECEIVED_AT.astimezone(timezone(timedelta(hours=2)))).isoformat()
    result = validate(payload(timestamp=local))
    assert result.timestamp_source == SOURCE_PAYLOAD
    assert result.timestamp.utcoffset() == timedelta(0)
    assert isoformat_z(result.timestamp).endswith("Z")
