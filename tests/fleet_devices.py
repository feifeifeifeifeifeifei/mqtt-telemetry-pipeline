"""The devices compose.yaml runs, one container each, named after its device.

A module of its own, not a test module: the compose and smoke tests both
import this, and test_smoke.py probes the broker port as soon as it is
imported.
"""

FLEET_DEVICES = ("gateway-01", "gateway-02", "gateway-03")
