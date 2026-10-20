"""One end-to-end test against the running compose stack.

Everything else in the suite runs without a broker, which is what keeps it fast
and deterministic. This one exists because the pieces can each be correct while
the wiring between them is not - a wrong topic, wrong credentials or a
subscribe in the wrong place would pass every unit test in the project.

Start the stack first with `docker compose up -d --build --wait` (--build, or
compose reuses images built from older code): the test subscribes through the
broker port compose publishes on 127.0.0.1 and waits for the simulator's
telemetry. It skips itself when nothing is listening on that port, so `pytest`
still passes on a machine with no Docker.
"""
from __future__ import annotations

import os
import socket
import time

import pytest
from fastapi.testclient import TestClient

from api import create_app
from telemetry.ingest import BrokerConfig, TelemetryIngestor
from telemetry.store import TelemetryStore

CONFIG = BrokerConfig.from_env()
# The simulator's identity: the same variable and default it reads itself.
EXPECTED_DEVICE = os.environ.get("SIM_DEVICE_ID", "gateway-01")
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


@pytest.mark.smoke
def test_real_telemetry_flows_from_the_broker_through_to_the_api():
    store = TelemetryStore(history_window=100)
    ingestor = TelemetryIngestor(store, CONFIG)
    client = TestClient(create_app(store, ingestor))

    with client:
        assert client.get("/api/v1/system-status").status_code in (200, 503)

        # Wait for exactly what is asserted below: three accepted CPU
        # temperatures. Counting messages instead would race the simulator's
        # faults, which can reject that field or the whole message. running()
        # stays None until the first accepted reading.
        deadline = time.monotonic() + WAIT_SECONDS
        while (
            (stats := store.running("cpu_temperature_c")) is None or stats.count < 3
        ) and time.monotonic() < deadline:
            time.sleep(0.2)

        assert stats is not None and stats.count >= 3, (
            "no telemetry arrived; is the simulator running (docker compose up -d --build --wait)? "
            "Check the topic and the credentials on both sides."
        )
        assert ingestor.broker_connected is True
        assert ingestor.handler_errors == 0

        status = client.get("/api/v1/system-status")
        assert status.status_code == 200
        body = status.json()
        assert body["device_id"] == EXPECTED_DEVICE
        assert body["readings"]["cpu_temperature_c"]["value"] is not None

        statistics = client.get("/api/v1/statistics").json()
        assert statistics["metrics"]["cpu_temperature_c"]["all_time"]["count"] >= 3

        anomalies = client.get("/api/v1/anomalies").json()
        assert len(anomalies["rules"]) == 6
        assert anomalies["messages_evaluated"] >= 3

    assert ingestor.broker_connected is False      # lifespan shut it down
