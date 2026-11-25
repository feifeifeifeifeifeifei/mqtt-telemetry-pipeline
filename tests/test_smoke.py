"""One end-to-end test against the running compose stack.

Everything else in the suite runs without a broker, which is what keeps it fast
and deterministic. This one exists because the pieces can each be correct while
the wiring between them is not - a wrong topic, wrong credentials or a
subscribe in the wrong place would pass every unit test in the project.

Start the stack first with `docker compose up -d --build --wait` (--build, or
compose reuses images built from older code): the test subscribes through the
broker port compose publishes on 127.0.0.1, waits for telemetry from every
gateway compose starts, and checks that each one's payloads name the device its
topic names. It skips itself when nothing is listening on that port, so
`pytest` still passes on a machine with no Docker.
"""
from __future__ import annotations

import socket
import time

import pytest
from fastapi.testclient import TestClient

from api import create_app
from telemetry.fleet import DeviceRegistry, FleetCounters
from telemetry.ingest import BrokerConfig, TelemetryIngestor
from tests.fleet_devices import FLEET_DEVICES

CONFIG = BrokerConfig.from_env()
# Generous on purpose: the loop below returns as soon as its condition holds.
WAIT_SECONDS = 30


def _broker_is_listening() -> bool:
    with socket.socket() as probe:
        probe.settimeout(1.0)
        return probe.connect_ex((CONFIG.host, CONFIG.port)) == 0


pytestmark = pytest.mark.skipif(
    not _broker_is_listening(),
    reason=(f"no MQTT broker on {CONFIG.host}:{CONFIG.port} "
            "(start it with docker compose up -d --build --wait)"),
)


def _accepted_cpu_temperatures(registry: DeviceRegistry, device_id: str) -> int:
    """The device's accepted CPU temperatures so far; 0 before its first message."""
    store = registry.get(device_id)
    # running() stays None until the first accepted reading.
    stats = None if store is None else store.running("cpu_temperature_c")
    return 0 if stats is None else stats.count


@pytest.mark.smoke
def test_real_telemetry_flows_from_the_broker_through_to_the_api():
    registry, counters = DeviceRegistry(history_window=100), FleetCounters()
    ingestor = TelemetryIngestor(registry, counters, CONFIG)
    # gateway-01 is the service's default device: the endpoints without a
    # device in their path answer for it.
    client = TestClient(create_app(registry, counters, ingestor, default_device_id=FLEET_DEVICES[0]))

    with client:
        assert client.get("/api/v1/system-status").status_code in (200, 503)

        # Wait for exactly what is asserted below: three accepted CPU
        # temperatures from every gateway. Counting messages instead would race
        # the simulator's faults, which can reject that field or the whole
        # message. No store is created up front: a gateway is in the registry
        # only once a message has arrived on its topic, so the device list
        # checked below can really come up short.
        deadline = time.monotonic() + WAIT_SECONDS
        while True:
            accepted = {device_id: _accepted_cpu_temperatures(registry, device_id)
                        for device_id in FLEET_DEVICES}
            if min(accepted.values()) >= 3 or time.monotonic() >= deadline:
                break
            time.sleep(0.2)

        assert min(accepted.values()) >= 3, (
            f"accepted CPU temperatures per gateway after {WAIT_SECONDS} s: {accepted}. "
            "Is every gateway running (docker compose up -d --build --wait)? "
            "Check the topic and the credentials on both sides."
        )
        assert ingestor.broker_connected is True
        assert ingestor.handler_errors == 0

        for device_id in FLEET_DEVICES:
            statistics = client.get(f"/api/v1/devices/{device_id}/statistics").json()
            # Every payload carries a device_id, and the service counts each one
            # that names another device than its topic does. Zero means this
            # gateway publishes under its own name, on its own topic.
            assert statistics["data_quality"]["cross_field"]["device_id_mismatch"] == 0, device_id
            assert statistics["metrics"]["cpu_temperature_c"]["all_time"]["count"] >= 3, device_id

            anomalies = client.get(f"/api/v1/devices/{device_id}/anomalies").json()
            assert len(anomalies["rules"]) == 6
            assert anomalies["messages_evaluated"] >= 3, device_id

        devices = client.get("/api/v1/devices").json()["devices"]
        assert sorted(row["device_id"] for row in devices) == sorted(FLEET_DEVICES)
        assert all(row["messages_received"] > 0 for row in devices), devices

        # The endpoints from before the fleet still answer, for gateway-01.
        status = client.get("/api/v1/system-status")
        assert status.status_code == 200
        body = status.json()
        assert body["device_id"] == FLEET_DEVICES[0]
        assert body["readings"]["cpu_temperature_c"]["value"] is not None

    assert ingestor.broker_connected is False      # lifespan shut it down
