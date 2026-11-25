"""compose.yaml's gateways, checked after YAML has merged their shared settings.

The gateways share one build definition and one set of settings through
anchors and `<<` merge keys. A merge key is shallow: a mapping merged in from
an anchor is replaced whole by a key of the same name in the service, never
combined with it. Reading the file as text cannot show that, so these tests
load it with PyYAML, which merges as Compose does, and check the result.
They need no Docker; the CI stack job starts the real containers.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from simulator.config import DEVICE_ID_PATTERN
from simulator.device import CYCLE_TICKS
from tests.fleet_devices import FLEET_DEVICES

COMPOSE = yaml.safe_load(
    (Path(__file__).resolve().parent.parent / "compose.yaml").read_text(encoding="utf-8")
)
SERVICES = COMPOSE["services"]


def environment(name: str) -> dict:
    return SERVICES[name]["environment"]


def test_the_services_built_from_the_simulator_are_the_fleet_in_order():
    built = [name for name, service in SERVICES.items() if service.get("build") == "./simulator"]
    assert built == list(FLEET_DEVICES)


def test_no_service_is_still_called_simulator():
    # It would publish as gateway-01, the simulator's default device id,
    # alongside the real gateway-01.
    assert "simulator" not in SERVICES


@pytest.mark.parametrize("name", FLEET_DEVICES)
def test_each_device_keeps_the_shared_settings_after_the_merge(name):
    # Each gateway merges x-simulator-env inside its own environment. Were the
    # shared settings in x-simulator, the gateway's environment would replace
    # them whole, and without MQTT_HOST the device looks for the broker on
    # localhost, which inside its container is nothing.
    settings = environment(name)
    assert settings.get("MQTT_HOST") == "broker"
    assert {"MQTT_USERNAME", "MQTT_PASSWORD", "SIM_FAULT_EVERY"} <= settings.keys()
    assert SERVICES[name]["depends_on"]["service"]["condition"] == "service_healthy"


@pytest.mark.parametrize("name", FLEET_DEVICES)
def test_each_device_is_named_after_its_service(name):
    device_id = environment(name)["SIM_DEVICE_ID"]
    assert device_id == name
    assert re.fullmatch(DEVICE_ID_PATTERN, device_id)


def test_the_seeds_differ_and_only_gateway_01_follows_sim_seed():
    seeds = [environment(name)["SIM_SEED"] for name in FLEET_DEVICES]
    # Compare what the containers get when SIM_SEED is unset: Compose replaces
    # ${NAME:-default} with its default, so the text "${SIM_SEED:-7}" and a
    # literal "7" are the same seed.
    defaults = [re.sub(r"\$\{\w+:-([^}]*)\}", r"\1", seed) for seed in seeds]
    assert len(set(defaults)) == len(defaults)
    # The text itself: YAML leaves ${...} alone, Compose fills it in. Seed 7
    # is the one the README's examples come from.
    assert environment("gateway-01")["SIM_SEED"] == "${SIM_SEED:-7}"
    # The others are fixed numbers, so setting SIM_SEED moves gateway-01 only.
    assert all(seed.isdigit() for seed in seeds[1:])


def test_the_devices_join_the_story_at_different_ticks_and_gateway_01_at_its_start():
    ticks = [int(environment(name)["SIM_START_TICK"]) for name in FLEET_DEVICES]
    assert len(set(ticks)) == len(ticks)
    assert all(0 <= tick < CYCLE_TICKS for tick in ticks)
    assert environment("gateway-01")["SIM_START_TICK"] == "0"


@pytest.mark.parametrize("name", FLEET_DEVICES)
def test_every_device_setting_is_a_string(name):
    # Quoted in the file, so YAML hands Compose the text "80", not a number.
    not_strings = {key: type(value).__name__
                   for key, value in environment(name).items() if not isinstance(value, str)}
    assert not_strings == {}
