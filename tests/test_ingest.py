from __future__ import annotations

import json
import logging
from dataclasses import replace

import paho.mqtt.client as mqtt
import pytest

from telemetry.fleet import DeviceRegistry, FleetCounters
from telemetry.ingest import BrokerConfig, TelemetryIngestor

GOOD = json.dumps({
    "battery_percent": 90.0, "cpu_temperature_c": 80.0, "cpu_usage_percent": 50.0,
    "device_id": "gateway-01", "disk_usage_percent": 50.0, "gpu_temperature_c": 60.0,
    "memory_total_mb": 4096, "memory_used_mb": 2048.0, "memory_used_percent": 50.0,
    "network_rx_kbps": 100.0, "network_tx_kbps": 100.0, "sequence": 1,
    "timestamp": "2026-10-19T12:00:00Z", "uptime_seconds": 1,
}).encode()

CONFIG = BrokerConfig(host="localhost", port=1883, username=None,
                      password=None, topic="devices/+/telemetry")


class FakeMessage:
    """Stands in for paho's message object: the handler reads .topic and .payload."""

    def __init__(self, payload: bytes, topic: str = "devices/gateway-01/telemetry") -> None:
        self.payload = payload
        self.topic = topic


class FakeReasonCode:
    def __init__(self, is_failure: bool) -> None:
        self.is_failure = is_failure

    def __str__(self) -> str:
        return "failure" if self.is_failure else "Success"


class FakeClient:
    def __init__(self) -> None:
        self.subscribed: list[str] = []

    def subscribe(self, topic: str) -> None:
        self.subscribed.append(topic)


def bare_ingestor(config: BrokerConfig = CONFIG) -> TelemetryIngestor:
    """An ingestor for the tests that never deliver a message."""
    return TelemetryIngestor(DeviceRegistry(history_window=10), FleetCounters(), config)


@pytest.fixture
def pair():
    # The device's store is created up front, so a test can hold it - or
    # sabotage it - before the first message. The ingestor then finds it under
    # the topic's id like any other.
    registry = DeviceRegistry(history_window=100)
    return registry.get_or_create("gateway-01"), TelemetryIngestor(registry, FleetCounters(), CONFIG)


def test_a_good_payload_reaches_the_store(pair):
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(GOOD))
    assert store.latest().readings["cpu_temperature_c"] == 80.0
    assert ingestor.handler_errors == 0


def test_a_truncated_payload_is_counted_not_raised(pair):
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(GOOD[: len(GOOD) // 2]))
    assert store.quality()["messages_undecodable"] == 1
    assert ingestor.handler_errors == 0


def test_an_integer_json_loads_refuses_is_counted_as_undecodable(pair):
    # Past Python's 4300-digit int limit json.loads raises a plain ValueError,
    # not a JSONDecodeError. Had that escaped validation, the catch-all would
    # only bump handler_errors and the message would never be counted at all.
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(b'{"uptime_seconds": ' + b"9" * 4301 + b"}"))
    assert store.messages_received == 1
    assert store.quality()["messages_undecodable"] == 1
    assert ingestor.handler_errors == 0


def test_json_nested_100000_deep_is_counted_whatever_the_decoder_does(pair):
    # Whether the C decoder copes with 100_000 levels depends on the version and
    # the platform: 3.14 limits it by stack size, so it raises RecursionError on
    # Windows and parses on Linux. Either way the message must be counted - as
    # undecodable, or, once parsed into a list, as wrong-shape - and must never
    # reach the catch-all.
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(b"[" * 100_000 + b"]" * 100_000))
    quality = store.quality()
    assert store.messages_received == 1
    assert quality["messages_undecodable"] + quality["messages_wrong_shape"] == 1
    assert ingestor.handler_errors == 0


@pytest.mark.parametrize("payload", [b"[1,2,3]", b"null", b'"hello"', b"42"])
def test_valid_json_that_is_not_an_object_is_counted_as_wrong_shape(pair, payload):
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(payload))
    assert store.quality()["messages_wrong_shape"] == 1


def test_an_unexpected_handler_failure_is_logged_and_counted_but_never_raised(pair, caplog):
    store, ingestor = pair

    def explode(_message):
        raise RuntimeError("something nobody predicted")

    store.record = explode  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        ingestor.on_message(None, None, FakeMessage(GOOD))     # must not raise
    assert ingestor.handler_errors == 1
    assert "something nobody predicted" in caplog.text


def test_the_raw_payload_is_logged_so_the_cause_can_be_read_off(pair, caplog):
    store, ingestor = pair
    store.record = lambda _m: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        ingestor.on_message(None, None, FakeMessage(GOOD))
    assert "cpu_temperature_c" in caplog.text


def test_the_loop_survives_a_bad_message_and_keeps_processing(pair):
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(b"{garbage"))
    ingestor.on_message(None, None, FakeMessage(GOOD))
    assert store.latest() is not None
    assert store.messages_received == 2


def test_the_failure_log_names_the_topic(pair, caplog):
    store, ingestor = pair
    store.record = lambda _m: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]
    with caplog.at_level(logging.ERROR):
        ingestor.on_message(None, None, FakeMessage(GOOD))
    assert "topic='devices/gateway-01/telemetry'" in caplog.text


class UnreadableTopicMessage:
    """A message whose .topic raises when read."""

    payload = GOOD

    @property
    def topic(self) -> str:
        raise RuntimeError("topic unreadable")


def test_reading_the_topic_is_inside_the_guard(pair, caplog):
    # And the failure log must not read it a second time: that read would
    # raise again, this time out of the callback and into paho's loop.
    _, ingestor = pair
    with caplog.at_level(logging.ERROR):
        ingestor.on_message(None, None, UnreadableTopicMessage())     # must not raise
    assert ingestor.handler_errors == 1
    assert "topic unreadable" in caplog.text
    assert "topic=None" in caplog.text


def test_a_broken_payload_is_stamped_with_the_same_receipt_time(pair, monkeypatch):
    # One clock reading per message serves both write paths. Left to itself,
    # record_payload_error would read the clock again, later.
    store, ingestor = pair
    ticks = iter([100.0, 200.0, 300.0])
    monkeypatch.setattr("telemetry.ingest.time.monotonic", lambda: next(ticks))
    ingestor.on_message(None, None, FakeMessage(b"{garbage"))
    assert store.last_seen_mono() == 100.0


# ----------------------------------------------------- one store per device
# The topic names the device, and each device has a store of its own. On one
# shared store two devices corrupt each other: B's low uptime reads as a reboot
# of A, and B's normal temperature ends A's overheating before it has lasted
# long enough to count. Interleaving the two is the point of these tests.

TOPIC_A = "devices/gw-a/telemetry"
TOPIC_B = "devices/gw-b/telemetry"


def reading_from(device_id: str, **fields) -> bytes:
    """GOOD as sent by `device_id`, with `fields` changed."""
    return json.dumps(dict(json.loads(GOOD), device_id=device_id, **fields)).encode()


@pytest.fixture
def fleet():
    registry = DeviceRegistry(max_devices=2, history_window=100)
    counters = FleetCounters()
    return registry, counters, TelemetryIngestor(registry, counters, CONFIG)


def test_previous_uptime_comes_from_the_same_device(fleet):
    # B's 5 in between is neither a reboot of B nor a reason to miss A's.
    registry, _, ingestor = fleet
    ingestor.handle_message(TOPIC_A, reading_from("gw-a", uptime_seconds=500))
    ingestor.handle_message(TOPIC_B, reading_from("gw-b", uptime_seconds=5))
    ingestor.handle_message(TOPIC_A, reading_from("gw-a", uptime_seconds=10))
    assert registry.get("gw-a").quality()["cross_field"]["uptime_regressed"] == 1
    assert registry.get("gw-b").quality()["cross_field"]["uptime_regressed"] == 0


def test_interleaved_devices_do_not_regress_each_other_s_uptime(fleet):
    registry, _, ingestor = fleet
    for step in range(6):
        ingestor.handle_message(TOPIC_A, reading_from("gw-a", uptime_seconds=500 + step))
        ingestor.handle_message(TOPIC_B, reading_from("gw-b", uptime_seconds=10 + step))
    for device_id in ("gw-a", "gw-b"):
        quality = registry.get(device_id).quality()
        assert quality["messages_received"] == 6
        assert quality["cross_field"]["uptime_regressed"] == 0


def test_one_device_s_normal_readings_do_not_end_another_s_overheating(fleet):
    registry, _, ingestor = fleet
    for step in range(6):
        ingestor.handle_message(TOPIC_A, reading_from("gw-a", uptime_seconds=500 + step, cpu_temperature_c=95.0))
        ingestor.handle_message(TOPIC_B, reading_from("gw-b", uptime_seconds=10 + step, cpu_temperature_c=60.0))
    hot = [episode for episode in registry.get("gw-a").anomalies() if episode.metric == "cpu_temperature_c"]
    assert len(hot) == 1
    assert (hot[0].ongoing, hot[0].occurrences) == (True, 6)
    assert registry.get("gw-b").anomalies() == []


def test_a_broken_payload_is_counted_against_the_device_in_its_topic(fleet):
    # The topic is read before the payload is decoded, so a payload with no
    # readable device_id still lands on - and creates - the device that sent it.
    registry, _, ingestor = fleet
    ingestor.handle_message(TOPIC_A, GOOD[: len(GOOD) // 2])
    ingestor.handle_message(TOPIC_B, b"[1,2,3]")
    a, b = registry.get("gw-a").quality(), registry.get("gw-b").quality()
    assert (a["messages_undecodable"], a["messages_wrong_shape"]) == (1, 0)
    assert (b["messages_undecodable"], b["messages_wrong_shape"]) == (0, 1)


def test_a_payload_naming_another_device_is_stored_under_its_topic_and_flagged(fleet):
    registry, _, ingestor = fleet
    ingestor.handle_message(TOPIC_A, reading_from("gw-b"))
    store = registry.get("gw-a")
    assert store.latest().device_id == "gw-b"
    assert store.quality()["cross_field"]["device_id_mismatch"] == 1
    assert registry.get("gw-b") is None


@pytest.mark.parametrize(
    "topic",
    ["devices/gw-a/status", "devices/.hidden/telemetry", "devices/gw-a/telemetry/extra", "devices/+/telemetry"],
    ids=["other-suffix", "invalid-id", "extra-level", "literal-plus"],
)
def test_a_topic_that_names_no_device_is_counted_and_stores_nothing(fleet, topic):
    registry, counters, ingestor = fleet
    ingestor.on_message(None, None, FakeMessage(GOOD, topic=topic))
    assert counters.snapshot() == {
        "messages_received": 1, "messages_invalid_topic": 1, "messages_over_device_limit": 0,
    }
    assert registry.snapshot() == []
    assert ingestor.handler_errors == 0


def test_a_topic_that_is_not_utf8_is_an_invalid_topic_not_a_handler_error(fleet):
    # A real paho message: it decodes .topic on every read and raises
    # UnicodeDecodeError for these bytes. MQTT forbids such a topic, but paho
    # still hands the message to on_message.
    registry, counters, ingestor = fleet
    message = mqtt.MQTTMessage(topic=b"devices/gw-\xff/telemetry")
    message.payload = GOOD
    ingestor.on_message(None, None, message)
    assert counters.snapshot() == {
        "messages_received": 1, "messages_invalid_topic": 1, "messages_over_device_limit": 0,
    }
    assert registry.snapshot() == []
    assert ingestor.handler_errors == 0


def test_over_the_cap_a_new_device_is_counted_and_warned_about_once(fleet, caplog):
    registry, counters, ingestor = fleet            # room for two devices
    ingestor.handle_message(TOPIC_A, reading_from("gw-a"))
    ingestor.handle_message(TOPIC_B, reading_from("gw-b"))
    with caplog.at_level(logging.WARNING, logger="telemetry.ingest"):
        for device_id in ("gw-c", "gw-d", "gw-c"):
            ingestor.handle_message(f"devices/{device_id}/telemetry", reading_from(device_id))
    warnings = [record.getMessage() for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "MAX_DEVICES=2" in warnings[0] and "'gw-c'" in warnings[0]
    assert counters.snapshot() == {
        "messages_received": 5, "messages_invalid_topic": 0, "messages_over_device_limit": 3,
    }
    assert [device_id for device_id, _ in registry.snapshot()] == ["gw-a", "gw-b"]
    # The cap only turns new devices away.
    ingestor.handle_message(TOPIC_A, reading_from("gw-a"))
    assert registry.get("gw-a").messages_received == 2


def test_on_connect_subscribes_so_a_reconnect_resubscribes(pair):
    # paho reconnects by itself and the broker forgets subscriptions; a
    # subscribe outside this callback stalls silently after the first reconnect.
    _, ingestor = pair
    client = FakeClient()
    ingestor.on_connect(client, None, None, FakeReasonCode(False), None)
    assert client.subscribed == ["devices/+/telemetry"]
    assert ingestor.broker_connected is True


def test_connectivity_is_claimed_only_once_the_subscribe_went_through(pair):
    # paho re-raises an exception from on_connect and its network loop ends.
    # The compose healthcheck reads broker_connected, so it must not stay
    # green for a service that will never receive a message.
    class RefusingClient:
        def subscribe(self, topic: str) -> None:
            raise ValueError("Invalid topic.")

    _, ingestor = pair
    with pytest.raises(ValueError):
        ingestor.on_connect(RefusingClient(), None, None, FakeReasonCode(False), None)
    assert ingestor.broker_connected is False


def test_a_refused_connection_does_not_subscribe_or_claim_connectivity(pair):
    _, ingestor = pair
    client = FakeClient()
    ingestor.on_connect(client, None, None, FakeReasonCode(True), None)
    assert client.subscribed == []
    assert ingestor.broker_connected is False


def test_disconnect_clears_the_connected_flag(pair):
    _, ingestor = pair
    ingestor.on_connect(FakeClient(), None, None, FakeReasonCode(False), None)
    ingestor.on_disconnect(None, None, None, FakeReasonCode(False), None)
    assert ingestor.broker_connected is False


def test_broker_config_defaults_match_the_documented_values():
    config = BrokerConfig.from_env({})
    assert config == BrokerConfig(
        host="localhost", port=1883, username=None,
        password=None, topic="devices/+/telemetry",
    )


def test_empty_credentials_mean_an_anonymous_client():
    # compose passes an unset ${MQTT_USERNAME:-} through as an empty string.
    config = BrokerConfig.from_env({"MQTT_USERNAME": "", "MQTT_PASSWORD": ""})
    assert (config.username, config.password) == (None, None)


def test_a_password_without_a_username_fails_at_startup():
    # Credentials are only sent when a username is set, so this password would
    # otherwise be dropped without a word and the broker would see an anonymous
    # client.
    with pytest.raises(ValueError, match="MQTT_PASSWORD is set but MQTT_USERNAME is not"):
        BrokerConfig.from_env({"MQTT_PASSWORD": "p"})


def test_the_topic_comes_from_mqtt_topic():
    # The simulator reads the same variable, so one setting moves both ends.
    config = BrokerConfig.from_env({"MQTT_TOPIC": "fleet/site-a/+/telemetry"})
    assert config.topic == "fleet/site-a/+/telemetry"


@pytest.mark.parametrize(
    "raw",
    ["devices/+/telemetry ", " devices/+/telemetry", "\tdevices/+/telemetry\n"],
    ids=["trailing", "leading", "tabs-and-newline"],
)
def test_whitespace_around_the_topic_filter_is_trimmed(raw):
    # check_topic_filter cannot see this one: the levels still look right, so a
    # trailing space passes validation and the service subscribes to a filter no
    # publisher matches - connected, healthy and deaf. The other settings are
    # already trimmed, except MQTT_USERNAME and MQTT_PASSWORD, which are passed
    # through as typed; this one was not.
    assert BrokerConfig.from_env({"MQTT_TOPIC": raw}).topic == "devices/+/telemetry"


def test_trimming_does_not_turn_a_blank_topic_into_the_default():
    # "" is compose's "unset" and takes the default; whitespace alone is a typo
    # and must still fail by name. Collapsing one into the other would hide it.
    with pytest.raises(ValueError, match="MQTT_TOPIC"):
        BrokerConfig.from_env({"MQTT_TOPIC": "   "})


def test_an_empty_topic_means_the_default():
    # As with the credentials, compose hands ${MQTT_TOPIC:-} over as "". paho
    # refuses to subscribe to "", and inside on_connect that stops its loop.
    assert BrokerConfig.from_env({"MQTT_TOPIC": ""}).topic == "devices/+/telemetry"


@pytest.mark.parametrize(
    "topic",
    ["devices/gateway-01/telemetry", "devices/#", "devices//telemetry", "  "],
    ids=["single-device-topic", "hash", "empty-level", "blank"],
)
def test_a_topic_filter_that_names_no_device_fails_at_startup(topic):
    # The old single-device default is in this list on purpose: subscribed
    # to as is, it would deliver messages and route every one of them to no
    # device. Failing here names the variable instead.
    with pytest.raises(ValueError, match="MQTT_TOPIC"):
        BrokerConfig.from_env({"MQTT_TOPIC": topic})


def test_broker_config_reads_every_documented_variable():
    config = BrokerConfig.from_env({
        "MQTT_HOST": "broker", "MQTT_PORT": "1884", "MQTT_USERNAME": "u",
        "MQTT_PASSWORD": "p", "MQTT_TOPIC": "t/+",
    })
    assert config == BrokerConfig(host="broker", port=1884, username="u", password="p", topic="t/+")


# --------------------------------------------------------------- paho wiring
# Everything above exercises message handling, which never touches paho. These
# pin the client setup itself: without them `connect_async` silently reverts to
# a blocking `connect`, which fails container startup on a cold broker and makes
# the system-status 503 ("no telemetry received yet") unreachable.


class RecordingClient:
    """Stands in for mqtt.Client, recording what the ingestor asks it to do."""

    def __init__(self, callback_api_version=None) -> None:
        self.callback_api_version = callback_api_version
        self.credentials: tuple[str, str] | None = None
        self.calls: list[tuple] = []
        self.on_connect = self.on_message = self.on_disconnect = None

    def username_pw_set(self, username: str, password: str) -> None:
        self.credentials = (username, password)

    def connect_async(self, host: str, port: int) -> None:
        self.calls.append(("connect_async", host, port))

    def connect(self, host: str, port: int) -> None:      # must never be used
        self.calls.append(("connect", host, port))

    def loop_start(self) -> None:
        self.calls.append(("loop_start",))

    def loop_stop(self) -> None:
        self.calls.append(("loop_stop",))

    def disconnect(self) -> None:
        self.calls.append(("disconnect",))


@pytest.fixture
def recorded(monkeypatch):
    import paho.mqtt.client as mqtt

    from telemetry import ingest as ingest_module

    monkeypatch.setattr(ingest_module.mqtt, "Client", RecordingClient)
    ingestor = bare_ingestor()
    return ingestor, ingestor._client, mqtt


def test_the_client_uses_the_v2_callback_api(recorded):
    _, client, mqtt = recorded
    assert client.callback_api_version is mqtt.CallbackAPIVersion.VERSION2


def _client_for(monkeypatch, config: BrokerConfig) -> RecordingClient:
    from telemetry import ingest as ingest_module

    monkeypatch.setattr(ingest_module.mqtt, "Client", RecordingClient)
    return bare_ingestor(config)._client


def test_without_a_username_the_client_connects_anonymously(monkeypatch):
    client = _client_for(monkeypatch, replace(CONFIG, username=None, password=None))
    assert client.credentials is None


def test_configured_credentials_are_passed_to_the_client(monkeypatch):
    client = _client_for(monkeypatch, replace(CONFIG, username="u", password="p"))
    assert client.credentials == ("u", "p")


def test_all_three_callbacks_are_bound(recorded):
    ingestor, client, _ = recorded
    assert client.on_connect == ingestor.on_connect
    assert client.on_message == ingestor.on_message
    assert client.on_disconnect == ingestor.on_disconnect


def test_start_connects_asynchronously_and_stop_shuts_down_cleanly(recorded):
    # connect_async, never connect: a blocking connect raises on a cold broker,
    # which fails startup and exits the container instead of serving the 503.
    ingestor, client, _ = recorded
    ingestor.start()
    ingestor.stop()
    assert client.calls == [
        ("connect_async", "localhost", 1883),
        ("loop_start",),
        ("loop_stop",),
        ("disconnect",),
    ]


def test_receipt_time_is_taken_from_a_monotonic_clock(pair, monkeypatch):
    # A wall clock jumps - NTP, DST, a manual change - and every age_seconds and
    # stale flag downstream is computed from this number.
    store, ingestor = pair
    monkeypatch.setattr("telemetry.ingest.time.monotonic", lambda: 4242.0)
    monkeypatch.setattr("telemetry.ingest.time.time", lambda: 9999.0)
    ingestor.on_message(None, None, FakeMessage(GOOD))
    assert store.latest().received_mono == 4242.0


def test_the_logged_payload_is_truncated(pair, caplog):
    # 200 as a literal: importing RAW_LOG_LIMIT would make this tautological and
    # let an off-by-one through.
    store, ingestor = pair
    store.record = lambda _m: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]
    padded = GOOD[:-1] + b',"padding":"' + b"x" * 500 + b'"}'
    with caplog.at_level(logging.ERROR):
        ingestor.on_message(None, None, FakeMessage(padded))
    assert repr(padded[:200]) in caplog.text
    assert repr(padded[:201]) not in caplog.text


@pytest.mark.parametrize("env", [{"MQTT_HOST": ""}, {"MQTT_HOST": "   "}, {"MQTT_PORT": ""}])
def test_an_empty_broker_variable_means_unset_not_a_crash(env):
    # compose turns an unset variable into an empty string. "" reached
    # int() and raised at import time, before the lifespan hook the 503
    # design depends on; "" as a host got as far as paho's "Invalid host".
    config = BrokerConfig.from_env(env)
    assert config.host == "localhost" and config.port == 1883


@pytest.mark.parametrize("port, fragment", [("abc", "not a whole number"), ("0", "1-65535"), ("70000", "1-65535")])
def test_an_unusable_port_is_rejected_by_name(port, fragment):
    with pytest.raises(ValueError, match=fragment):
        BrokerConfig.from_env({"MQTT_PORT": port})


def test_the_watchdog_names_a_hostname_that_does_not_resolve():
    # The failure that broke a live demo: a container recreated onto a
    # different network, where the service goes quiet and paho logs one DEBUG
    # line that reads the same as a refused port.
    config = BrokerConfig(host="no-such-broker.invalid", port=1883,
                          username=None, password=None, topic="t")
    reason = bare_ingestor(config)._diagnose()
    assert "does not resolve" in reason and "no-such-broker.invalid" in reason


def test_the_watchdog_tells_a_closed_port_apart_from_an_unresolvable_name():
    config = BrokerConfig(host="127.0.0.1", port=9, username=None, password=None, topic="t")
    reason = bare_ingestor(config)._diagnose()
    assert "does not resolve" not in reason
    assert "127.0.0.1:9" in reason
