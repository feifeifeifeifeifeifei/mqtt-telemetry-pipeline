"""Tests for telemetry.fleet.

Not in mutmut's test selection on purpose: mutmut only mutates validation.py,
and these tests would only slow every mutant down.
"""
from __future__ import annotations

import pathlib
import threading
import time

import pytest

from telemetry import fleet
from telemetry.store import DEFAULT_HISTORY_WINDOW, TelemetryStore

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
        "$share/group/devices/+/telemetry",
        "$queue/devices/+/telemetry",
    ],
    ids=["empty", "no-plus", "two-plus-levels", "hash-level", "plus-and-hash",
         "hash-inside-a-level", "empty-level-no-plus", "empty-level-with-plus",
         "leading-slash", "trailing-slash", "plus-glued-before", "plus-glued-after",
         "whole-plus-and-glued-plus", "shared-subscription", "queue-subscription"],
)
def test_invalid_topic_filters_raise_naming_the_variable(topic_filter):
    # Each case trips one rule the others would miss: plus-and-hash has exactly
    # one "+" level, empty-level-with-plus too, and plus-glued-before has a "+"
    # but no level that is only "+". The two shared subscriptions have exactly
    # one "+" level as well, but the broker delivers their messages under the
    # topic without the prefix, which then has fewer levels than the filter.
    with pytest.raises(ValueError, match="MQTT_TOPIC"):
        fleet.check_topic_filter(topic_filter)


@pytest.mark.parametrize(
    "topic_filter, prefix",
    [
        ("$share/group/devices/+/telemetry", "'$share/group/'"),
        ("$queue/devices/+/telemetry", "'$queue/'"),
        ("$share", "'$share/'"),
    ],
    ids=["shared-subscription", "queue-subscription", "bare-share"],
)
def test_a_shared_subscription_error_names_the_whole_prefix_to_drop(topic_filter, prefix):
    # Dropping only "$share/" would leave "group/devices/+/telemetry", which
    # passes every other rule and then fits no topic the broker delivers.
    with pytest.raises(ValueError) as raised:
        fleet.check_topic_filter(topic_filter)
    assert f"without the {prefix} prefix" in str(raised.value)


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


# --------------------------------------------------------------- registry


def test_the_same_id_always_gets_the_same_store():
    registry = fleet.DeviceRegistry(max_devices=2, history_window=5)
    first = registry.get_or_create("gateway-01")
    assert isinstance(first, TelemetryStore)
    assert registry.get_or_create("gateway-01") is first


def test_each_device_gets_a_store_of_its_own():
    registry = fleet.DeviceRegistry(max_devices=2, history_window=5)
    a = registry.get_or_create("a")
    b = registry.get_or_create("b")
    assert a is not b
    a.record_payload_error("undecodable", received_mono=1.0)
    assert (a.messages_received, b.messages_received) == (1, 0)


def test_a_full_registry_refuses_new_devices_but_still_serves_known_ones():
    registry = fleet.DeviceRegistry(max_devices=2, history_window=5)
    a = registry.get_or_create("a")
    b = registry.get_or_create("b")
    assert registry.get_or_create("c") is None
    assert registry.get_or_create("a") is a
    assert registry.get_or_create("b") is b
    assert registry.get("c") is None
    assert [device_id for device_id, _ in registry.snapshot()] == ["a", "b"]


def test_get_never_creates():
    registry = fleet.DeviceRegistry(max_devices=1, history_window=5)
    assert registry.get("a") is None
    assert registry.snapshot() == []
    # The read did not take the only slot.
    store = registry.get_or_create("a")
    assert store is not None
    assert registry.get("a") is store


def test_snapshot_is_sorted_by_id_and_detached():
    registry = fleet.DeviceRegistry(max_devices=5, history_window=5)
    for device_id in ("b", "a", "C"):
        registry.get_or_create(device_id)
    snapshot = registry.snapshot()
    # Plain string order, so upper case sorts before lower case.
    assert [device_id for device_id, _ in snapshot] == ["C", "a", "b"]
    assert all(store is registry.get(device_id) for device_id, store in snapshot)
    registry.get_or_create("d")
    assert [device_id for device_id, _ in snapshot] == ["C", "a", "b"]
    snapshot.clear()
    assert len(registry.snapshot()) == 4


def test_max_devices_is_read_only():
    registry = fleet.DeviceRegistry(max_devices=3, history_window=5)
    assert registry.max_devices == 3
    with pytest.raises(AttributeError):
        registry.max_devices = 4


@pytest.mark.parametrize("max_devices", [0, -1])
def test_a_cap_below_one_is_refused(max_devices):
    with pytest.raises(ValueError, match="MAX_DEVICES"):
        fleet.DeviceRegistry(max_devices=max_devices, history_window=5)


def test_new_stores_get_the_registry_history_window():
    registry = fleet.DeviceRegistry(history_window=7)
    assert registry.max_devices == fleet.DEFAULT_MAX_DEVICES
    assert registry.get_or_create("a").window_capacity() == 7


def test_history_window_is_read_only():
    registry = fleet.DeviceRegistry(max_devices=3, history_window=7)
    assert registry.history_window == 7
    with pytest.raises(AttributeError):
        registry.history_window = 8


def test_history_window_is_read_once_when_the_registry_is_built(monkeypatch):
    # Every device gets the same window size, whatever happens to the
    # environment after startup.
    monkeypatch.setenv("HISTORY_WINDOW", "7")
    registry = fleet.DeviceRegistry()
    monkeypatch.setenv("HISTORY_WINDOW", "9")
    assert registry.get_or_create("a").window_capacity() == 7
    assert registry.history_window == 7


def test_from_env_defaults():
    registry = fleet.DeviceRegistry.from_env({})
    assert registry.max_devices == 32
    assert registry.get_or_create("a").window_capacity() == DEFAULT_HISTORY_WINDOW


def test_from_env_reads_both_settings_from_the_mapping_it_is_given(monkeypatch):
    monkeypatch.setenv("MAX_DEVICES", "9")
    monkeypatch.setenv("HISTORY_WINDOW", "9")
    registry = fleet.DeviceRegistry.from_env({"MAX_DEVICES": "5", "HISTORY_WINDOW": "7"})
    assert registry.max_devices == 5
    assert registry.get_or_create("a").window_capacity() == 7


def test_from_env_reads_the_process_environment_by_default(monkeypatch):
    monkeypatch.setenv("MAX_DEVICES", "3")
    monkeypatch.setenv("HISTORY_WINDOW", "11")
    registry = fleet.DeviceRegistry.from_env()
    assert registry.max_devices == 3
    assert registry.get_or_create("a").window_capacity() == 11


@pytest.mark.parametrize("raw", ["", "  "], ids=["empty", "blank"])
def test_an_empty_max_devices_means_the_default(raw):
    # compose passes an unset variable through as an empty string.
    assert fleet.DeviceRegistry.from_env({"MAX_DEVICES": raw}).max_devices == 32


@pytest.mark.parametrize("raw", ["0", "-1", "abc", "1.5", "32 devices"])
def test_a_bad_max_devices_fails_at_startup(raw):
    with pytest.raises(ValueError, match="MAX_DEVICES"):
        fleet.DeviceRegistry.from_env({"MAX_DEVICES": raw})


# ------------------------------------------------------ registry under races


def run_together(count, target):
    """Release `count` threads at once through a barrier; return target(i) per thread."""
    barrier = threading.Barrier(count)
    results = [None] * count
    errors: list[BaseException] = []

    def worker(index):
        try:
            barrier.wait(timeout=10)
            results[index] = target(index)
        except BaseException as exc:      # noqa: BLE001 - recorded, then asserted on
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert errors == []
    return results


@pytest.fixture
def slow_stores(monkeypatch):
    """Building a store takes 10 ms, so racing threads overlap inside get_or_create.

    Without the delay one thread finishes its check-then-insert before the next
    is scheduled, and a registry with no lock passes both tests below. With it,
    removing the lock makes both fail.
    """
    real = fleet.TelemetryStore

    def slow(*args, **kwargs):
        time.sleep(0.01)
        return real(*args, **kwargs)

    monkeypatch.setattr(fleet, "TelemetryStore", slow)


def test_racing_creators_of_one_device_share_one_store(slow_stores):
    registry = fleet.DeviceRegistry(max_devices=32, history_window=5)
    stores = run_together(32, lambda _: registry.get_or_create("gateway-01"))
    assert stores[0] is not None
    assert all(store is stores[0] for store in stores)
    assert registry.snapshot() == [("gateway-01", stores[0])]


def test_racing_creators_cannot_push_the_registry_past_its_cap(slow_stores):
    registry = fleet.DeviceRegistry(max_devices=5, history_window=5)
    ids = [f"dev-{index:02d}" for index in range(40)]
    stores = run_together(len(ids), lambda index: registry.get_or_create(ids[index]))
    created = {ids[index] for index, store in enumerate(stores) if store is not None}
    assert len(created) == 5
    assert {device_id for device_id, _ in registry.snapshot()} == created
    assert all(registry.get(ids[index]) is store
               for index, store in enumerate(stores) if store is not None)


# ---------------------------------------------------------- fleet counters


def test_fleet_counters_start_at_zero():
    assert fleet.FleetCounters().snapshot() == {
        "messages_received": 0,
        "messages_invalid_topic": 0,
        "messages_over_device_limit": 0,
    }


def test_each_fleet_counter_counts_only_its_own_event():
    counters = fleet.FleetCounters()
    for _ in range(3):
        counters.record_received()
    for _ in range(2):
        counters.record_invalid_topic()
    counters.record_over_device_limit()
    # Different counts on purpose: one each would pass with two branches swapped.
    assert counters.snapshot() == {
        "messages_received": 3,
        "messages_invalid_topic": 2,
        "messages_over_device_limit": 1,
    }


def test_only_the_first_over_limit_message_asks_for_the_warning():
    counters = fleet.FleetCounters()
    assert [counters.record_over_device_limit() for _ in range(3)] == [True, False, False]
    assert counters.snapshot()["messages_over_device_limit"] == 3


def test_a_fleet_counters_snapshot_is_detached():
    counters = fleet.FleetCounters()
    counters.record_received()
    snapshot = counters.snapshot()
    snapshot["messages_received"] = 99
    counters.record_received()
    assert snapshot["messages_received"] == 99
    assert counters.snapshot()["messages_received"] == 2


class CountingLock:
    """A real lock that counts how often it is taken."""

    def __init__(self):
        self._lock = threading.Lock()
        self.acquisitions = 0

    def __enter__(self):
        self.acquisitions += 1
        return self._lock.__enter__()

    def __exit__(self, *exc_info):
        return self._lock.__exit__(*exc_info)


FLEET_COUNTERS_METHODS = ("record_received", "record_invalid_topic", "record_over_device_limit", "snapshot")

# The registry needs the same guard. Without it, removing `with self._lock` from
# get() or snapshot() left all 652 tests green: neither iterates a container in
# Python, so a missing lock there is a torn read, not an exception, and only
# counting acquisitions can see it.
DEVICE_REGISTRY_METHODS = ("get_or_create", "get", "snapshot")
# Read without the lock on purpose: both are set in __init__ and never written
# again, which is the same exemption TelemetryStore.window_capacity documents.
DEVICE_REGISTRY_LOCK_FREE = ("max_devices", "history_window")


def test_the_lock_check_covers_every_public_device_registry_method():
    # A method added later must join one of the two lists above, or this fails.
    public = {
        name for name, value in vars(fleet.DeviceRegistry).items()
        if not name.startswith("_") and not isinstance(value, classmethod)
    }
    assert public == set(DEVICE_REGISTRY_METHODS) | set(DEVICE_REGISTRY_LOCK_FREE)


@pytest.mark.parametrize("method", DEVICE_REGISTRY_METHODS)
def test_every_device_registry_method_takes_the_lock_exactly_once(method):
    registry = fleet.DeviceRegistry(max_devices=4)
    registry.get_or_create("gateway-01")            # one device to look up
    lock = registry._lock = CountingLock()          # type: ignore[assignment]
    argument = ("gateway-01",) if method != "snapshot" else ()
    getattr(registry, method)(*argument)
    assert lock.acquisitions == 1
    assert not lock._lock.locked()


@pytest.mark.parametrize("name", DEVICE_REGISTRY_LOCK_FREE)
def test_the_lock_free_registry_properties_really_are_immutable(name):
    # The exemption above is only sound while nothing writes these after
    # __init__. Reading them must take no lock at all.
    registry = fleet.DeviceRegistry(max_devices=4)
    lock = registry._lock = CountingLock()          # type: ignore[assignment]
    getattr(registry, name)
    assert lock.acquisitions == 0
    assert f"self._{name} =" not in (
        pathlib.Path(fleet.__file__).read_text(encoding="utf-8")
        .split("def get_or_create", 1)[1]
    ), f"_{name} is written after __init__; it can no longer be read without the lock"


def test_the_lock_check_covers_every_public_fleet_counters_method():
    # A method added later must join the list below, or this fails.
    public = {name for name in vars(fleet.FleetCounters) if not name.startswith("_")}
    assert public == set(FLEET_COUNTERS_METHODS)


@pytest.mark.parametrize("method", FLEET_COUNTERS_METHODS)
def test_every_fleet_counters_method_takes_the_lock_exactly_once(method):
    # This, not the threaded test below, is what fails when a method loses
    # its `with self._lock`.
    counters = fleet.FleetCounters()
    lock = counters._lock = CountingLock()       # type: ignore[assignment]
    getattr(counters, method)()
    assert lock.acquisitions == 1
    assert not lock._lock.locked()


def test_racing_counters_smoke_check():
    # A smoke check only: it cannot detect a missing lock. On a GIL build the
    # interpreter never switches threads inside `+= 1` or between it and the
    # `== 1` check, so this passes with the lock removed; only a free-threaded
    # build could fail it.
    counters = fleet.FleetCounters()

    def burst(_):
        firsts = 0
        for _ in range(200):
            counters.record_received()
            counters.record_invalid_topic()
            firsts += counters.record_over_device_limit()
        return firsts

    firsts = run_together(16, burst)
    assert sum(firsts) == 1
    assert counters.snapshot() == {
        "messages_received": 3200,
        "messages_invalid_topic": 3200,
        "messages_over_device_limit": 3200,
    }


@pytest.mark.parametrize("value", ["1", "32", str(fleet.MAX_DEVICES_CEILING)])
def test_a_device_cap_inside_the_bounds_is_accepted(value):
    assert fleet.DeviceRegistry.from_env({"MAX_DEVICES": value}).max_devices == int(value)


@pytest.mark.parametrize("value", ["0", "-1", str(fleet.MAX_DEVICES_CEILING + 1), "1000000"])
def test_a_device_cap_outside_the_bounds_is_refused_by_name(value):
    # The cap is the memory bound - DeviceRegistry's docstring says so. Without
    # a ceiling, one extra typed zero makes that sentence false and nothing
    # says so until the host runs out of memory.
    with pytest.raises(ValueError, match="MAX_DEVICES"):
        fleet.DeviceRegistry.from_env({"MAX_DEVICES": value})


def test_the_device_cap_ceiling_is_the_documented_one_and_is_inclusive():
    # The README says 1024, so the number is pinned here rather than only
    # through the constant. The ceiling itself is allowed; one above is not.
    assert fleet.MAX_DEVICES_CEILING == 1024
    ceiling = str(fleet.MAX_DEVICES_CEILING)
    assert fleet.DeviceRegistry.from_env({"MAX_DEVICES": ceiling}).max_devices == 1024
    with pytest.raises(ValueError, match="MAX_DEVICES"):
        fleet.DeviceRegistry.from_env({"MAX_DEVICES": str(fleet.MAX_DEVICES_CEILING + 1)})
