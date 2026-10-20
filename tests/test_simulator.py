from __future__ import annotations

import json
import logging
import os
import random
import subprocess
import sys
import threading
import types
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from simulator import publisher
from simulator.__main__ import main
from simulator.config import DEFAULT_DEVICE_ID, DEFAULT_TOPIC, Config
from simulator.device import (
    CYCLE_TICKS,
    KEYFRAMES,
    NOISE,
    REBOOT_TICK,
    build_message,
    encode,
    interpolate,
    reading,
    uptime_seconds,
)
from simulator.faults import CATALOGUE, fault_for
from simulator.stream import Frame, generate, make_frame, narrate
from telemetry.validation import PLAUSIBLE_RANGES

START = datetime(2026, 10, 19, 8, 0, tzinfo=timezone.utc)
DEFAULT = Config.from_env({})

# ------------------------------------------------------------------- config


def test_config_defaults_match_the_documented_values():
    assert Config.from_env({}) == Config(
        host="localhost", port=1883, username=None, password=None,
        topic="devices/gateway-01/telemetry", device_id="gateway-01",
        rate_hz=1.0, seed=7, fault_every=10,
    )
    # Literals, not the constants: the service hard-codes the same topic, so a
    # silent change here would leave the two sides talking past each other.
    assert (DEFAULT_TOPIC, DEFAULT_DEVICE_ID) == ("devices/gateway-01/telemetry", "gateway-01")


def test_config_reads_every_documented_variable():
    config = Config.from_env({
        "MQTT_HOST": "broker", "MQTT_PORT": "1884", "MQTT_USERNAME": "u",
        "MQTT_PASSWORD": "p", "MQTT_TOPIC": "t", "SIM_DEVICE_ID": "d",
        "SIM_RATE_HZ": "2.5", "SIM_SEED": "9", "SIM_FAULT_EVERY": "0",
    })
    assert config == Config(
        host="broker", port=1884, username="u", password="p", topic="t",
        device_id="d", rate_hz=2.5, seed=9, fault_every=0,
    )


def test_empty_credentials_mean_anonymous():
    # compose passes an unset ${MQTT_USERNAME:-} through as an empty string.
    config = Config.from_env({"MQTT_USERNAME": "", "MQTT_PASSWORD": ""})
    assert (config.username, config.password) == (None, None)


def test_an_empty_topic_means_the_default():
    # The service's convention: compose hands ${MQTT_TOPIC:-} over as "", and
    # paho refuses to publish to "", so the first frame would crash the run.
    assert Config.from_env({"MQTT_TOPIC": ""}).topic == "devices/gateway-01/telemetry"


def test_a_password_without_a_username_is_refused():
    with pytest.raises(ValueError, match="MQTT_USERNAME"):
        Config.from_env({"MQTT_PASSWORD": "p"})


def test_an_empty_device_id_is_refused():
    with pytest.raises(ValueError, match="SIM_DEVICE_ID"):
        Config.from_env({"SIM_DEVICE_ID": ""})


@pytest.mark.parametrize("rate", ["0.05", "11", "nan"])
def test_a_rate_outside_the_supported_range_is_refused(rate):
    with pytest.raises(ValueError, match="SIM_RATE_HZ"):
        Config.from_env({"SIM_RATE_HZ": rate})


@pytest.mark.parametrize("rate", ["0.1", "10"])
def test_the_rate_limits_themselves_are_accepted(rate):
    assert Config.from_env({"SIM_RATE_HZ": rate}).rate_hz == float(rate)


@pytest.mark.parametrize("every", ["1", "-1"])
def test_a_fault_spacing_of_one_or_below_zero_is_refused(every):
    with pytest.raises(ValueError, match="SIM_FAULT_EVERY"):
        Config.from_env({"SIM_FAULT_EVERY": every})


def test_a_non_numeric_port_fails_at_startup():
    with pytest.raises(ValueError):
        Config.from_env({"MQTT_PORT": "abc"})


# ------------------------------------------------------------------- device


def clean_reading(tick: int, seed: int = 7, rate_hz: float = 1.0) -> dict:
    """What the stream feeds reading() for this seed and tick, with no fault applied."""
    return reading(tick, random.Random(f"{seed}:{tick}:noise"), rate_hz)


def test_every_track_loops_without_a_jump():
    for field, points in KEYFRAMES.items():
        ticks = [tick for tick, _ in points]
        assert ticks[0] == 0 and ticks[-1] == CYCLE_TICKS, field
        # Strictly increasing: a repeated tick would divide by zero in interpolate.
        assert all(a < b for a, b in zip(ticks, ticks[1:])), field
        assert points[0][1] == points[-1][1], field


def test_every_track_has_a_noise_amplitude():
    assert set(NOISE) == set(KEYFRAMES)


def test_interpolate_draws_straight_lines_and_refuses_ticks_outside_the_table():
    points = [(0, 10.0), (10, 20.0), (20, 0.0)]
    assert interpolate(points, 5) == 15.0
    assert interpolate(points, 10) == 20.0
    assert interpolate(points, 15) == 10.0
    with pytest.raises(ValueError):
        interpolate(points, 21)


def test_clean_readings_are_always_plausible_and_memory_is_consistent():
    # The service must never reject or flag a clean message, or a demo would
    # show data-quality problems the simulator did not inject.
    for seed in range(1, 21):
        for tick in range(2 * CYCLE_TICKS):
            values = clean_reading(tick, seed)
            assert set(values) == set(PLAUSIBLE_RANGES)
            for field, (low, high) in PLAUSIBLE_RANGES.items():
                assert low <= values[field] <= high, (seed, tick, field, values[field])
            percent_from_mb = values["memory_used_mb"] / values["memory_total_mb"] * 100
            assert abs(percent_from_mb - values["memory_used_percent"]) < 0.01, (seed, tick)


@pytest.mark.parametrize("rate_hz", [0.5, 1.0, 2.0])
def test_uptime_only_goes_backwards_at_the_reboot(rate_hz):
    uptimes = [uptime_seconds(tick, rate_hz) for tick in range(2 * CYCLE_TICKS)]
    drops = [tick for tick in range(1, len(uptimes)) if uptimes[tick] < uptimes[tick - 1]]
    assert drops == [REBOOT_TICK, REBOOT_TICK + CYCLE_TICKS]
    assert uptimes[0] == 0
    # The README promises uptime back at 0 at the reboot, in every cycle.
    assert uptimes[REBOOT_TICK] == uptimes[REBOOT_TICK + CYCLE_TICKS] == 0
    # Real seconds, not ticks: at 2 Hz, 179 ticks after power-on is 89 s.
    assert uptimes[REBOOT_TICK - 1] == int((REBOOT_TICK - 1) / rate_hz)


def test_the_backup_burst_is_three_samples_above_the_cpu_threshold():
    # The cpu_usage rule needs 5 consecutive samples over 95, so the service
    # must drop these 3; the contract tests check that it does.
    for seed in range(1, 21):
        for cycle_start in (0, CYCLE_TICKS):
            usage = [clean_reading(cycle_start + tick, seed)["cpu_usage_percent"] for tick in range(210, 215)]
            assert usage[0] < 20 and usage[4] < 20, (seed, usage)
            assert all(value > 95 for value in usage[1:4]), (seed, usage)


def test_a_message_carries_identity_time_and_readings_in_insertion_order():
    now = datetime(2026, 10, 19, 8, 0, 57, tzinfo=timezone.utc)
    message = build_message(57, now, "gateway-01", {"cpu_usage_percent": 97.64})
    assert list(message) == ["device_id", "sequence", "timestamp", "cpu_usage_percent"]
    assert encode(message) == (
        b'{"device_id": "gateway-01", "sequence": 57, '
        b'"timestamp": "2026-10-19T08:00:57+00:00", "cpu_usage_percent": 97.64}'
    )


# ------------------------------------------------------------------- faults


def test_the_catalogue_rotates_in_the_documented_order():
    # The README timeline ("truncated at 0:10-0:19, ...") is this order.
    assert [fault.name for fault in CATALOGUE] == [
        "sensor-timeout", "truncated", "null-reading", "rtc-reset",
        "number-as-text", "adc-saturated", "batched-array", "nan-reading",
        "epoch-millis", "stale-memory-counter", "boolean-reading", "bit-flip",
        "huge-counter", "counter-wrap", "http-date", "firmware-v2",
    ]
    assert [fault.name for fault in CATALOGUE if fault.expect == "clean"] == ["firmware-v2"]


@pytest.mark.parametrize("every", [2, 10, 25])
def test_each_block_holds_exactly_one_entry_in_catalogue_order(every):
    for block in range(100):
        ticks = range(block * every, (block + 1) * every)
        entries = [fault for fault in (fault_for(tick, 7, every) for tick in ticks) if fault is not None]
        assert entries == [CATALOGUE[block % len(CATALOGUE)]], block


def test_the_seed_moves_the_entry_within_its_block():
    def positions(seed: int) -> list[int]:
        return [tick for tick in range(1000) if fault_for(tick, seed, 10) is not None]

    assert positions(1) != positions(2)


def test_fault_injection_can_be_switched_off():
    assert all(fault_for(tick, 7, 0) is None for tick in range(1000))


# ------------------------------------------------------------------- stream


def payloads(config: Config, count: int = 2 * CYCLE_TICKS) -> list[bytes]:
    return [frame.payload for frame in generate(config, count, START)]


def test_the_same_seed_gives_the_same_bytes():
    assert payloads(DEFAULT) == payloads(DEFAULT)


def test_a_different_seed_gives_different_bytes():
    assert payloads(DEFAULT) != payloads(replace(DEFAULT, seed=8))


def test_switching_faults_off_leaves_every_clean_frame_byte_identical():
    # Noise and faults draw from separate generators, so the fault switch
    # cannot shift the noise of the frames it leaves alone.
    off = payloads(replace(DEFAULT, fault_every=0))
    clean = [frame for frame in generate(DEFAULT, 2 * CYCLE_TICKS, START) if frame.fault is None]
    assert len(clean) == 2 * CYCLE_TICKS - 2 * CYCLE_TICKS // DEFAULT.fault_every
    assert all(off[frame.tick] == frame.payload for frame in clean)


def test_frames_are_stamped_on_the_planned_schedule():
    stamps = [frame.sent_at for frame in generate(replace(DEFAULT, rate_hz=2.0), 3, START)]
    assert stamps == [START, START + timedelta(seconds=0.5), START + timedelta(seconds=1)]


def test_seed_7_frame_57_is_the_documented_example():
    # The README quotes this message; it must stay byte-for-byte reproducible.
    frame = make_frame(57, START + timedelta(seconds=57), DEFAULT)
    assert frame.fault is None
    assert frame.payload == (
        b'{"device_id": "gateway-01", "sequence": 57, "timestamp": "2026-10-19T08:00:57+00:00", '
        b'"cpu_usage_percent": 97.64, "cpu_temperature_c": 86.66, "gpu_temperature_c": 69.04, '
        b'"memory_used_percent": 93.91, "network_rx_kbps": 16081.4, "network_tx_kbps": 3196.39, '
        b'"disk_usage_percent": 89.64, "battery_percent": 31.73, "memory_total_mb": 4096, '
        b'"memory_used_mb": 3846.6, "uptime_seconds": 57}'
    )


def test_narration_marks_power_on_only_at_the_very_first_tick():
    assert narrate(Frame(0, START, b"", None)) == [
        "tick 0: gateway powered on, uptime starts at 0",
        "tick 0: idle: background load only",
    ]
    # Later cycles open with uptime at 60, not at a fresh boot.
    assert narrate(Frame(240, START, b"", None)) == ["tick 240: idle: background load only"]
    assert narrate(Frame(1, START, b"", None)) == []


def test_narration_names_the_fault_and_the_verdict_to_expect():
    # Seed 7 puts block 5's entry, adc-saturated, at tick 56.
    frame = make_frame(56, START + timedelta(seconds=56), DEFAULT)
    assert frame.fault is CATALOGUE[5]
    assert narrate(frame) == [
        "tick 56: fault adc-saturated (thermistor wire broke, 8-bit ADC reads full scale) "
        "-> expect cpu_temperature_c:out-of-range",
    ]


def test_narration_flags_the_backup_burst_in_every_cycle():
    assert narrate(Frame(450, START, b"", None)) == [
        "tick 450: backup job: CPU bursts for 3 samples, too short to alert (rule needs 5)",
    ]


# ---------------------------------------------------------------------- cli

SETTINGS = ("MQTT_HOST", "MQTT_PORT", "MQTT_USERNAME", "MQTT_PASSWORD", "MQTT_TOPIC",
            "SIM_DEVICE_ID", "SIM_RATE_HZ", "SIM_SEED", "SIM_FAULT_EVERY")


@pytest.fixture
def default_env(monkeypatch):
    """main() reads the real environment; clear it so the defaults apply."""
    for name in SETTINGS:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("start", ["2026-10-19T16:00:00+08:00", "2026-10-19T08:00:00"])
def test_a_dry_run_start_is_normalised_to_utc(tmp_path, default_env, start):
    # Clean frames carry UTC as in live mode, whatever offset --start uses, and
    # a naive start is taken as UTC. 240 frames run through every catalogue
    # entry, the time-zone-converting ones included.
    out = tmp_path / "x.jsonl"
    assert main(["--count", "240", "--start", start, "--out", str(out)]) == 0
    lines = out.read_bytes().splitlines()
    assert len(lines) == 240
    assert json.loads(lines[0])["timestamp"] == "2026-10-19T08:00:00+00:00"


def test_a_dry_run_writes_exactly_the_generated_frames(tmp_path, default_env):
    out = tmp_path / "x.jsonl"
    main(["--count", "240", "--start", "2026-10-19T08:00:00+00:00", "--out", str(out)])
    assert out.read_bytes() == b"".join(frame.payload + b"\n" for frame in generate(DEFAULT, 240, START))


def test_without_out_the_frames_go_to_stdout_and_the_story_to_the_log(default_env, capsysbinary, caplog):
    caplog.set_level(logging.INFO, logger="simulator")
    assert main(["--count", "3", "--start", "2026-10-19T08:00:00+00:00"]) == 0
    assert capsysbinary.readouterr().out == b"".join(
        frame.payload + b"\n" for frame in generate(DEFAULT, 3, START)
    )
    assert "tick 0: gateway powered on, uptime starts at 0" in caplog.messages


ROOT = Path(__file__).resolve().parent.parent


def process_env(**overrides: str) -> dict[str, str]:
    """An environment for the simulator as its own process, the way its image runs it.

    In-process tests cannot see two things. Under pytest the root logger already
    has handlers, so main()'s logging.basicConfig changes nothing; and
    pyproject.toml puts src/ on the path, so an import of the service would go
    unnoticed - the image has no src/. Without PYTHONPATH, neither does the
    child process.
    """
    unset = (*SETTINGS, "LOG_LEVEL", "PYTHONPATH")
    env = {name: value for name, value in os.environ.items() if name not in unset}
    return {**env, **overrides}


def run_simulator(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, *args], cwd=ROOT, env=env, capture_output=True, timeout=60)


def test_a_dry_run_process_writes_frames_to_stdout_and_the_story_to_stderr():
    # It also fails if anything in simulator/ imports the service.
    result = run_simulator(
        ["-m", "simulator", "--count", "240", "--start", "2026-10-19T08:00:00+00:00"], process_env()
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert result.stdout == b"".join(frame.payload + b"\n" for frame in generate(DEFAULT, 240, START))
    assert b"tick 0: gateway powered on" in result.stderr


def test_log_level_quiets_the_narration():
    # Lower case on purpose: main() upper-cases it for logging.
    result = run_simulator(
        ["-m", "simulator", "--count", "3", "--start", "2026-10-19T08:00:00+00:00"],
        process_env(LOG_LEVEL="warning"),
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert result.stdout == b"".join(frame.payload + b"\n" for frame in generate(DEFAULT, 3, START))
    assert result.stderr == b""


def test_a_dry_run_imports_neither_paho_nor_the_service(tmp_path):
    out = str(tmp_path / "x.jsonl")
    code = (
        "import json, sys\n"
        "from simulator.__main__ import main\n"
        f"main(['--count', '240', '--out', {out!r}])\n"
        "print(json.dumps(sorted(m for m in sys.modules if m.split('.')[0] in ('paho', 'telemetry'))))\n"
    )
    result = run_simulator(["-c", code], process_env())
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert json.loads(result.stdout) == []


def test_a_reader_that_stops_early_ends_the_dry_run_quietly():
    # As in `| head -1`: 2000 frames overfill the pipe, so the reader closes it
    # while the simulator is still writing.
    with subprocess.Popen(
        [sys.executable, "-m", "simulator", "--count", "2000", "--start", "2026-10-19T08:00:00+00:00"],
        cwd=ROOT, env=process_env(LOG_LEVEL="WARNING"), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    ) as process:
        first = process.stdout.readline()
        process.stdout.close()
        stderr = process.stderr.read()
        assert process.wait(timeout=60) == 1
    assert json.loads(first)["sequence"] == 0
    assert stderr == b""     # no BrokenPipeError traceback, no "Exception ignored" at exit


def test_the_help_calls_the_count_n(capsys):
    with pytest.raises(SystemExit):
        main(["--help"])
    assert "[--count N]" in capsys.readouterr().out


def test_live_mode_hands_over_to_the_publisher_and_exits_cleanly_on_ctrl_c(default_env, monkeypatch):
    # A stand-in module: the real publisher needs a broker, and this checks
    # only the hand-over and that Ctrl+C (docker stop's SIGINT) exits with 0.
    seen = []

    def run(config):
        seen.append(config)
        raise KeyboardInterrupt

    fake = types.ModuleType("simulator.publisher")
    fake.run = run
    monkeypatch.setitem(sys.modules, "simulator.publisher", fake)
    assert main([]) == 0
    assert seen == [DEFAULT]


# ---------------------------------------------------------------- publisher


class FakeReasonCode:
    def __init__(self, is_failure: bool) -> None:
        self.is_failure = is_failure

    def __str__(self) -> str:
        return "Not authorized" if self.is_failure else "Success"


class FakePublishInfo:
    def __init__(self, rc: int) -> None:
        self.rc = rc


class FakeClock:
    """Stands in for the time module inside run(): sleeping moves the clock, nothing waits.

    The sleep after the last wanted frame raises KeyboardInterrupt, which is
    how Ctrl+C and docker stop's SIGINT reach run() for real.
    """

    def __init__(self, frames: int) -> None:
        self.now = 1000.0
        self.frames = frames
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(round(seconds, 6))
        if len(self.sleeps) == self.frames:
            raise KeyboardInterrupt
        self.now += seconds


class RecordingClient:
    """Stands in for mqtt.Client, recording what the publisher asks it to do.

    It plays the broker as well: loop_start() delivers each of `answers` to
    on_connect from a timer thread, 50 ms apart, as paho's network thread
    would - so run() really has to wait for the connection.
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.callback_api_version = None
        self.credentials: tuple[str, str | None] | None = None
        self.calls: list[tuple] = []
        self.payloads: list[bytes] = []
        self.on_connect = None
        self.answers = [FakeReasonCode(False)]
        self.publish_rc = publisher.mqtt.MQTT_ERR_SUCCESS
        self.publish_seconds: list[float] = []   # how long each publish takes on the fake clock
        self.timers: list[threading.Timer] = []

    def username_pw_set(self, username: str, password: str | None = None) -> None:
        self.credentials = (username, password)

    def connect_async(self, host: str, port: int) -> None:
        self.calls.append(("connect_async", host, port))

    def connect(self, host: str, port: int) -> None:      # must never be used
        self.calls.append(("connect", host, port))

    def loop_start(self) -> None:
        self.calls.append(("loop_start",))
        for number, reason_code in enumerate(self.answers, start=1):
            timer = threading.Timer(0.05 * number, self._answer, args=(reason_code,))
            self.timers.append(timer)
            timer.start()

    def _answer(self, reason_code: FakeReasonCode) -> None:
        self.calls.append(("on_connect", str(reason_code)))
        self.on_connect(self, None, None, reason_code, None)

    def publish(self, topic: str, payload: bytes, qos: int = 0) -> FakePublishInfo:
        self.calls.append(("publish", topic, qos))
        self.payloads.append(payload)
        if self.publish_seconds:
            self.clock.now += self.publish_seconds.pop(0)
        return FakePublishInfo(self.publish_rc)

    def loop_stop(self) -> None:
        self.calls.append(("loop_stop",))

    def disconnect(self) -> None:
        self.calls.append(("disconnect",))


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock(frames=3)
    monkeypatch.setattr(publisher, "time", fake)
    return fake


@pytest.fixture
def client(monkeypatch, clock):
    """The client the publisher will build, ready for the test to inspect."""
    recording = RecordingClient(clock)

    def build(callback_api_version):
        recording.callback_api_version = callback_api_version
        return recording

    monkeypatch.setattr(publisher.mqtt, "Client", build)
    yield recording
    for timer in recording.timers:
        timer.join()


def run_until_interrupted(config: Config) -> None:
    with pytest.raises(KeyboardInterrupt):
        publisher.run(config)


def test_without_a_username_the_publisher_connects_anonymously(client):
    assert publisher.make_client(DEFAULT) is client
    assert client.callback_api_version is publisher.mqtt.CallbackAPIVersion.VERSION2
    assert client.credentials is None


def test_configured_credentials_are_passed_to_the_client(client):
    publisher.make_client(replace(DEFAULT, username="u", password="p"))
    assert client.credentials == ("u", "p")


@pytest.mark.parametrize(("now", "expected"), [(10.2, 11.0), (15.0, 15.0)], ids=["on-time", "behind"])
def test_the_next_deadline_is_one_period_on_but_never_in_the_past(now, expected):
    # Behind schedule (the host slept), the next frame goes out now and the
    # schedule restarts from there: no burst of catch-up frames.
    assert publisher.next_deadline(10.0, 1.0, now) == expected


def test_run_connects_in_the_background_waits_for_the_broker_and_cleans_up_on_ctrl_c(client):
    # connect_async, never connect: a blocking connect raises while the broker
    # is still starting. Nothing is published before the broker has answered.
    run_until_interrupted(DEFAULT)
    assert client.calls == [
        ("connect_async", "localhost", 1883),
        ("loop_start",),
        ("on_connect", "Success"),
        ("publish", "devices/gateway-01/telemetry", 0),
        ("publish", "devices/gateway-01/telemetry", 0),
        ("publish", "devices/gateway-01/telemetry", 0),
        ("loop_stop",),
        ("disconnect",),
    ]


def test_a_refused_connection_is_logged_and_publishing_waits_for_a_later_success(client, caplog):
    client.answers = [FakeReasonCode(True), FakeReasonCode(False)]
    run_until_interrupted(DEFAULT)
    refusal = ("simulator.publisher", logging.ERROR, "broker refused the connection: Not authorized")
    assert refusal in caplog.record_tuples
    assert client.calls[2:5] == [
        ("on_connect", "Not authorized"),
        ("on_connect", "Success"),
        ("publish", "devices/gateway-01/telemetry", 0),
    ]


def test_run_publishes_the_stream_stamped_with_the_real_clock(client):
    config = replace(DEFAULT, fault_every=0)
    before = datetime.now(timezone.utc)
    run_until_interrupted(config)
    after = datetime.now(timezone.utc)
    assert len(client.payloads) == 3
    for tick, payload in enumerate(client.payloads):
        stamp = datetime.fromisoformat(json.loads(payload)["timestamp"])
        assert before <= stamp <= after
        # Byte for byte the frame the dry run and the contract tests compute.
        assert payload == make_frame(tick, stamp, config).payload


def test_pacing_absorbs_the_send_time_and_never_bursts_to_catch_up(client, clock):
    # 2 Hz, so a frame is due every 0.5 s. The second publish stalls for 2 s.
    client.publish_seconds = [0.1, 2.0, 0.1]
    run_until_interrupted(replace(DEFAULT, rate_hz=2.0))
    assert clock.sleeps == [0.4, 0.0, 0.4]


@pytest.mark.parametrize(("changes", "settings"), [
    ({}, "every 1.00 s, seed 7, one catalogue entry per 10 frames"),
    ({"rate_hz": 2.0, "fault_every": 0}, "every 0.50 s, seed 7, fault injection off"),
], ids=["defaults", "faults-off"])
def test_run_logs_its_settings_and_then_the_story(client, caplog, changes, settings):
    caplog.set_level(logging.INFO, logger="simulator")
    run_until_interrupted(replace(DEFAULT, **changes))
    assert caplog.messages[:3] == [
        "connected to localhost:1883",
        f"gateway-01 -> devices/gateway-01/telemetry {settings}",
        "tick 0: gateway powered on, uptime starts at 0",
    ]


def test_a_frame_paho_could_not_send_is_logged_and_the_story_goes_on(client, caplog):
    client.publish_rc = publisher.mqtt.MQTT_ERR_NO_CONN
    run_until_interrupted(DEFAULT)
    assert len(client.payloads) == 3
    assert "tick 0 not sent: The client is not currently connected." in caplog.messages


# ---------------------------------------------------------------- packaging


def test_the_simulator_image_pins_the_same_paho_as_the_service():
    # Two images, two requirements files: this keeps the device and the
    # service on the same MQTT client library.
    root = Path(__file__).resolve().parent.parent

    def paho(path: Path) -> list[str]:
        lines = path.read_text(encoding="utf-8").splitlines()
        return [line.strip() for line in lines if line.startswith("paho-mqtt")]

    service = paho(root / "requirements.txt")
    assert len(service) == 1 and service[0].startswith("paho-mqtt=="), service   # one exact pin
    assert paho(root / "simulator" / "requirements.txt") == service
