from __future__ import annotations

import json
import logging
from dataclasses import replace

import pytest

from telemetry.ingest import BrokerConfig, TelemetryIngestor
from telemetry.store import TelemetryStore

GOOD = json.dumps({
    "battery_percent": 90.0, "cpu_temperature_c": 80.0, "cpu_usage_percent": 50.0,
    "device_id": "gateway-01", "disk_usage_percent": 50.0, "gpu_temperature_c": 60.0,
    "memory_total_mb": 4096, "memory_used_mb": 2048.0, "memory_used_percent": 50.0,
    "network_rx_kbps": 100.0, "network_tx_kbps": 100.0, "sequence": 1,
    "timestamp": "2026-10-19T12:00:00Z", "uptime_seconds": 1,
}).encode()

CONFIG = BrokerConfig(host="localhost", port=1883, username=None,
                      password=None, topic="devices/gateway-01/telemetry")


class FakeMessage:
    """Stands in for paho's message object: the handler only reads .payload."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.topic = "devices/gateway-01/telemetry"


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


@pytest.fixture
def pair():
    store = TelemetryStore(history_window=100)
    return store, TelemetryIngestor(store, CONFIG)


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


@pytest.mark.parametrize(
    "payload",
    [b'{"uptime_seconds": ' + b"9" * 4301 + b"}", b"[" * 100_000 + b"]" * 100_000],
    ids=["integer-over-4300-digits", "nested-100000-deep"],
)
def test_a_payload_json_loads_refuses_is_counted_as_undecodable(pair, payload):
    # Both fail inside json.loads with something other than JSONDecodeError.
    # Had that escaped validation, the catch-all would only bump handler_errors
    # and the message would never appear in messages_received at all.
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(payload))
    assert store.messages_received == 1
    assert store.quality()["messages_undecodable"] == 1
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


def test_previous_uptime_is_threaded_through_so_regressions_are_flagged(pair):
    store, ingestor = pair
    ingestor.on_message(None, None, FakeMessage(GOOD.replace(b'"uptime_seconds": 1', b'"uptime_seconds": 500')))
    ingestor.on_message(None, None, FakeMessage(GOOD.replace(b'"uptime_seconds": 1', b'"uptime_seconds": 10')))
    assert store.quality()["cross_field"]["uptime_regressed"] == 1


def test_on_connect_subscribes_so_a_reconnect_resubscribes(pair):
    # paho reconnects by itself and the broker forgets subscriptions; a
    # subscribe outside this callback stalls silently after the first reconnect.
    _, ingestor = pair
    client = FakeClient()
    ingestor.on_connect(client, None, None, FakeReasonCode(False), None)
    assert client.subscribed == ["devices/gateway-01/telemetry"]
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
        password=None, topic="devices/gateway-01/telemetry",
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
    config = BrokerConfig.from_env({"MQTT_TOPIC": "devices/gateway-02/telemetry"})
    assert config.topic == "devices/gateway-02/telemetry"


def test_an_empty_topic_means_the_default():
    # As with the credentials, compose hands ${MQTT_TOPIC:-} over as "". paho
    # refuses to subscribe to "", and inside on_connect that stops its loop.
    assert BrokerConfig.from_env({"MQTT_TOPIC": ""}).topic == "devices/gateway-01/telemetry"


def test_broker_config_reads_every_documented_variable():
    config = BrokerConfig.from_env({
        "MQTT_HOST": "broker", "MQTT_PORT": "1884", "MQTT_USERNAME": "u",
        "MQTT_PASSWORD": "p", "MQTT_TOPIC": "t",
    })
    assert config == BrokerConfig(host="broker", port=1884, username="u", password="p", topic="t")


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
    store = TelemetryStore(history_window=10)
    ingestor = TelemetryIngestor(store, CONFIG)
    return ingestor, ingestor._client, mqtt


def test_the_client_uses_the_v2_callback_api(recorded):
    _, client, mqtt = recorded
    assert client.callback_api_version is mqtt.CallbackAPIVersion.VERSION2


def _client_for(monkeypatch, config: BrokerConfig) -> RecordingClient:
    from telemetry import ingest as ingest_module

    monkeypatch.setattr(ingest_module.mqtt, "Client", RecordingClient)
    return TelemetryIngestor(TelemetryStore(history_window=10), config)._client


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


def test_receipt_time_is_taken_from_a_monotonic_clock(monkeypatch):
    # A wall clock jumps - NTP, DST, a manual change - and every age_seconds and
    # stale flag downstream is computed from this number.
    store = TelemetryStore(history_window=10)
    ingestor = TelemetryIngestor(store, CONFIG)
    monkeypatch.setattr("telemetry.ingest.time.monotonic", lambda: 4242.0)
    monkeypatch.setattr("telemetry.ingest.time.time", lambda: 9999.0)
    ingestor.on_message(None, None, FakeMessage(GOOD))
    assert store.latest().received_mono == 4242.0


def test_the_logged_payload_is_truncated(caplog):
    # 200 as a literal: importing RAW_LOG_LIMIT would make this tautological and
    # let an off-by-one through.
    store = TelemetryStore(history_window=10)
    ingestor = TelemetryIngestor(store, CONFIG)
    store.record = lambda _m: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]
    padded = GOOD[:-1] + b',"padding":"' + b"x" * 500 + b'"}'
    with caplog.at_level(logging.ERROR):
        ingestor.on_message(None, None, FakeMessage(padded))
    assert repr(padded[:200]) in caplog.text
    assert repr(padded[:201]) not in caplog.text
