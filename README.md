# mqtt-telemetry-pipeline

A small end-to-end telemetry pipeline. A simulated edge gateway publishes system telemetry
over MQTT; a Python service subscribes, validates every reading, keeps statistics, groups
threshold violations into anomaly episodes and serves all of it over a small REST API. A
mosquitto broker sits between the two, and one `docker compose` command runs all three.

The simulator is scripted on purpose. Every four minutes it replays a compressed day —
load, cooling, a charger, a disk cleanup, a firmware reboot and a short backup burst — and
every ten seconds it injects one entry from a catalogue of fifteen faults and one benign
variant. Each entry states the verdict the service must reach, and a contract test feeds
every frame through the real validator, store and API to check it.

## Quick start

Needs Docker with Compose v2. Nothing else: no local Python, no broker to install.

```bash
docker compose up -d --build --wait
```

This builds the service and simulator images, starts them with an `eclipse-mosquitto`
broker, and returns once the service is subscribed and the simulator has started. The API
is then at `http://localhost:8000`, with interactive docs at `/docs`. Running it again is
harmless: compose only recreates what changed.

Follow the simulator's narration — every story beat, and every injected fault with the
verdict to expect:

```bash
docker compose logs -f simulator
```

To see the service before any telemetry arrives — the 503 shown under API — start from
nothing (`docker compose down` if the stack is up), bring up the broker and the service,
ask, then start the simulator:

```bash
docker compose up -d --build --wait broker service
```

```bash
curl -i localhost:8000/api/v1/system-status
```

```bash
docker compose up -d simulator
```

On Windows PowerShell 5.1 type `curl.exe`: plain `curl` there is an alias for
`Invoke-WebRequest`. A browser on `http://localhost:8000/docs` works everywhere.

The broker image ships the mosquitto command-line clients, so watching the raw stream needs
nothing installed:

```bash
docker compose exec broker mosquitto_sub -t devices/gateway-01/telemetry -v
```

`docker compose stop simulator` followed by a 30-second wait shows every reading go stale;
`docker compose down` stops and removes all three containers. The broker is published on
`127.0.0.1:1883` and the API on `127.0.0.1:8000`, so neither is reachable from other
machines.

## API

Base URL `http://localhost:8000`. Interactive docs at `/docs`.

| Method | Path | Returns | Other codes |
|---|---|---|---|
| `GET` | `/health` | Process liveness, broker connection, message count | — |
| `GET` | `/api/v1/system-status` | Freshest valid value **per field**, each with its own `as_of` and age | `503` before any telemetry |
| `GET` | `/api/v1/statistics` | Per metric: all-time count/min/max/mean/stdev + windowed median/p95, plus window capacity/fill and `data_quality` | — |
| `GET` | `/api/v1/statistics/top-temperatures?component=cpu\|gpu` | All-time 3 highest and 3 lowest readings with timestamps | `422` on an unknown component |
| `GET` | `/api/v1/anomalies` | Threshold violations grouped into episodes, plus the active rule set | — |

`/health` exists so compose can wait for readiness instead of sleeping a fixed time (see
Packaging). Every response below is real output of this code, produced by feeding the
default simulator stream (seed 7, first message at 08:00:00 UTC) through the service on its
1 Hz schedule, and trimmed to a few fields where the full body would be long. Readings are
deterministic per seed, so a live run shows the same values at the same point of the story;
only timestamps and ages differ. The commands end in `| jq .` only to pretty-print the JSON;
leave it off where jq is not installed.

**Current device state**, 1:40 into the story, at the peak of the video-analytics batch.
Every field appears every time, each with its own provenance, because the response is
stitched from the freshest valid reading per field rather than from one message.

```bash
curl -s localhost:8000/api/v1/system-status | jq .
```

```json
{
  "device_id": "gateway-01",
  "as_of": "2026-10-19T08:01:39Z",
  "readings": {
    "cpu_temperature_c": {"value": 91.22, "as_of": "2026-10-19T08:01:39Z", "age_seconds": 0.48,
                          "stale": false},
    "gpu_temperature_c": {"value": 79.4, "as_of": "2026-10-19T08:01:39Z", "age_seconds": 0.48,
                          "stale": false}
  },
  "oldest_reading_age_seconds": 0.48
}
```

A field whose newest valid reading is over 30 seconds old reports `"value": null,
"stale": true` **and keeps its real `age_seconds`** — "the last value was 412 s ago" is
more useful than "no value". A field that has never had a valid reading has no age to
report, so all three are null. When the payload timestamp was substituted, the field also
carries `"timestamp_source": "receipt-unparseable"` or `"receipt-implausible"`; the
normal case omits the key entirely.

Before the first message arrives the endpoint returns **503**, with enough to tell
"started a few seconds ago" from "misconfigured and has never received anything":

```json
{"detail": "no telemetry received yet", "broker_connected": true, "messages_received": 0,
 "service_uptime_seconds": 8.4}
```

**Statistics** after one full cycle (4:00). All-time figures are exact and independent of
retention; median and p95 come from the window and are labelled as such.

```bash
curl -s localhost:8000/api/v1/statistics | jq .
```

```json
{
  "window_samples": 3600,
  "window_filled": 235,
  "metrics": {
    "cpu_temperature_c": {
      "all_time": {"count": 233, "min": 49.65, "max": 91.4, "mean": 64.9402, "stdev": 16.5782},
      "window": {"count": 233, "median": 54.38, "p95": 91.22}
    }
  },
  "data_quality": {
    "messages_received": 240,
    "messages_undecodable": 3,
    "messages_wrong_shape": 2,
    "per_field": {
      "cpu_temperature_c": {"accepted": 233, "rejected": 2, "reasons": {"out-of-range": 2}}
    },
    "cross_field": {"timestamp_substituted": 4, "memory_inconsistent": 1, "uptime_regressed": 1},
    "recent_rejections": [
      {"at": "2026-10-19T08:00:43Z", "field": "cpu_usage_percent", "raw": "'99.29'",
       "reason": "not-a-number"},
      {"at": "2026-10-19T08:01:40Z", "field": "memory_used_percent", "raw": "False",
       "reason": "not-a-number"}
    ]
  }
}
```

Every count under `data_quality` can be read off the fault catalogue below: one cycle holds
24 catalogue slots, which put three undecodable payloads, two batched arrays, four bad
timestamps and one stale memory counter on the wire, and the firmware reboot at 3:00 adds
the `uptime_regressed`. The two `recent_rejections` shown (13 after a full cycle) are two
more catalogue entries: a number sent as text, and a boolean in a numeric field. The raw
value is kept verbatim so the cause reads off directly.

**Temperature extremes**, all-time and unaffected by window eviction — the highest from the
batch peak, the lowest from the idle stretch:

```bash
curl -s 'localhost:8000/api/v1/statistics/top-temperatures?component=cpu' | jq .
```

```json
{
  "component": "cpu",
  "metric": "cpu_temperature_c",
  "highest": [
    {"value": 91.4, "timestamp": "2026-10-19T08:01:28Z"},
    {"value": 91.39, "timestamp": "2026-10-19T08:01:15Z"},
    {"value": 91.37, "timestamp": "2026-10-19T08:01:46Z"}
  ],
  "lowest": [
    {"value": 49.65, "timestamp": "2026-10-19T08:00:14Z"},
    {"value": 49.68, "timestamp": "2026-10-19T08:00:02Z"},
    {"value": 49.7, "timestamp": "2026-10-19T08:00:16Z"}
  ]
}
```

`?component=tpu` returns **422** with the allowed values named, generated by FastAPI from
the parameter's type:

```json
{
  "detail": [
    {
      "type": "enum",
      "loc": ["query", "component"],
      "msg": "Input should be 'cpu' or 'gpu'",
      "input": "tpu",
      "ctx": {"expected": "'cpu' or 'gpu'"}
    }
  ]
}
```

**Anomalies**, grouped into episodes and shipped with the rules that produced them. At 1:56
the batch is over, but the chips are still cooling:

```bash
curl -s localhost:8000/api/v1/anomalies | jq .
```

```json
{
  "anomalies": [
    {"metric": "cpu_usage_percent", "condition": "above", "threshold": 95.0,
     "started_at": "2026-10-19T08:00:36Z", "ended_at": "2026-10-19T08:01:49Z",
     "duration_seconds": 73.0, "occurrences": 72, "peak_value": 99.47, "ongoing": false},
    {"metric": "cpu_temperature_c", "condition": "above", "threshold": 85.0,
     "started_at": "2026-10-19T08:00:57Z", "ended_at": "2026-10-19T08:01:55Z",
     "duration_seconds": 58.0, "occurrences": 57, "peak_value": 91.4, "ongoing": true}
  ],
  "rules": [
    {"metric": "cpu_temperature_c", "condition": "above", "threshold": 85.0, "consecutive": 3,
     "rationale": "Edge SoCs throttle around 85 C; three consecutive samples filter sensor noise."},
    {"metric": "battery_percent", "condition": "below", "threshold": 30.0, "consecutive": 1,
     "rationale": "Below 30% the device needs intervention; battery readings move smoothly, so no debounce."}
  ],
  "messages_evaluated": 113,
  "episodes_discarded": 0
}
```

`occurrences: 72` is 72 violating samples reported as one record. The episode spanned 74
messages: one had its CPU reading rejected (`number-as-text` at 0:43) and one never decoded
as an object (`batched-array` at 1:06), and neither split the episode — a missing reading is
not evidence that the condition ended. `ongoing: true` is the single most useful field here —
it separates "the CPU ran hot earlier" from "the CPU is hot right now". `rules` ships so an
empty `anomalies` list is unambiguous: a caller sees all six watched metrics with their
thresholds, not just the ones firing.

## The device simulator

`simulator/` stands in for a field gateway, `gateway-01`. It is its own package and its own
image, and it never imports the service: the two share an MQTT topic and nothing else, and
the contract between them lives in the tests.

**A demo device, not a statistical model.** Everything is scripted so that every feature of
the service has a moment that triggers it, and this README can say when. The story repeats
every 240 messages (four minutes at the default 1 Hz), and catalogue entries arrive one per
ten messages in a fixed rotation. Time is compressed: the battery drains 0.075% a second and
the disk gains three points in two and a half minutes, which takes hours on real hardware.

### The four-minute story

Times count from the first message at the default 1 Hz. Where a range is given, it is the
spread over seeds 1–300: noise moves a threshold crossing by a second or two.

| At | What happens (the narration) | What the API shows |
|---|---|---|
| before 0:00 | the service is subscribed, the simulator not started | `system-status` returns 503 |
| 0:00 | gateway powered on; idle, background load only | `system-status` 200 with `device_id: gateway-01`; `messages_received` climbing |
| 0:30 | video-analytics batch starts: CPU, memory and network climb | |
| 0:40–0:41 | CPU usage above 95% for the fifth message in a row | `/anomalies`: `cpu_usage_percent` ongoing |
| 0:57–0:58 | memory above 90% for the fifth message in a row | `memory_used_percent` ongoing |
| 0:58–0:59 | CPU above 85 °C for the third message in a row | `cpu_temperature_c` ongoing |
| 1:01–1:03 | GPU above 72 °C for the third message in a row | `gpu_temperature_c` ongoing |
| 1:16 | disk passes 90% | `disk_usage_percent` ongoing |
| **1:21** | battery below 30% | **all six rules ongoing at once**, until 1:50–1:51 |
| 1:50 | batch done: load drops, fans cool the chips | CPU-usage and memory episodes close at 1:51–1:52, the two temperatures at 1:55–1:58 |
| 2:00 | technician plugs in the charger | battery closes at 2:17 with `peak_value: 27.0`, the low point |
| 2:30 | operator deletes old video clips remotely: disk drops to 72% | disk closes at 2:31: all six episodes closed |
| 2:35 | downloading firmware update | network rx climbs to ~6 Mbps by 2:40 |
| 3:00 | rebooting to install firmware (uptime restarts at 0) | `cross_field.uptime_regressed` goes to 1; uptime in `system-status` back to 0 |
| 3:10 | charger unplugged | |
| 3:31–3:33 | backup job: CPU at about 97% for three messages | **no new episode**: the rule needs five |
| 4:00 | the story starts again | the catalogue carries on from entry 8 |

The charger and the cleanup are there on purpose. The battery rule's rationale says a device
below 30% needs intervention, and the disk rule's says a disk never recovers unaided, so in
the story both recover only when a person steps in. The backup burst at 3:31 is the
debounce made visible: three samples above 95% are real, and they are dropped because the
rule asks for five in a row. Its block's catalogue entry is always `adc-saturated` or
`counter-wrap`, so no fault ever touches those three CPU readings.

### The fault catalogue

The stream is cut into blocks of ten messages. Each block holds exactly one catalogue entry,
at a position the seed picks, and the entries rotate in this order. So the rate is exactly
one in ten, fifteen of every sixteen entries are real faults, and all sixteen have appeared
within 160 seconds — which random injection could not promise. First slots are for the first
rotation at 1 Hz; from 2:40 the rotation starts again.

| # | First slot | Entry | Real-world cause | Expected verdict |
|---:|---|---|---|---|
| 0 | 0:00–0:09 | `sensor-timeout` | GPU sensor did not answer | `gpu_temperature_c:missing` |
| 1 | 0:10–0:19 | `truncated` | radio dropped the tail of the packet | `payload:undecodable` |
| 2 | 0:20–0:29 | `null-reading` | fuel gauge timed out on the I2C bus | `battery_percent:null` |
| 3 | 0:30–0:39 | `rtc-reset` | RTC backup cell is flat, the clock restarted at 2000-01-01 with no time zone | `timestamp:receipt-implausible` |
| 4 | 0:40–0:49 | `number-as-text` | firmware put quotes around a number | `cpu_usage_percent:not-a-number` |
| 5 | 0:50–0:59 | `adc-saturated` | thermistor wire broke, 8-bit ADC reads full scale | `cpu_temperature_c:out-of-range` |
| 6 | 1:00–1:09 | `batched-array` | a single message was wrapped in a batch array | `payload:wrong-shape` |
| 7 | 1:10–1:19 | `nan-reading` | driver divided by zero | `gpu_temperature_c:not-finite` |
| 8 | 1:20–1:29 | `epoch-millis` | timestamp sent as JavaScript-style epoch milliseconds | `timestamp:receipt-unparseable` |
| 9 | 1:30–1:39 | `stale-memory-counter` | used memory was read before the cache was reclaimed | `flag:memory-inconsistent` |
| 10 | 1:40–1:49 | `boolean-reading` | a status bit was sent instead of a reading | `memory_used_percent:not-a-number` |
| 11 | 1:50–1:59 | `bit-flip` | line noise corrupted three bytes | `payload:undecodable` |
| 12 | 2:00–2:09 | `huge-counter` | the memory holding the uptime counter was corrupted | `uptime_seconds:not-finite` |
| 13 | 2:10–2:19 | `counter-wrap` | 32-bit byte counter wrapped around | `network_rx_kbps:out-of-range` |
| 14 | 2:20–2:29 | `http-date` | firmware used the HTTP date format | `timestamp:receipt-unparseable` |
| 15 | 2:30–2:39 | `firmware-v2` | new firmware adds fan_rpm and reports local time at +08:00 | `clean` |

A verdict reads `<field>:<reason>` (that reading is rejected), `payload:<reason>` (the whole
message is), `timestamp:<source>` (the timestamp is replaced by the receipt time),
`flag:<flag>` (a cross-field check fires) or `clean`. These are the strings the contract
test compares, so this table is also the test table. A few entries are there to reach a
particular branch:

- `truncated` and `bit-flip` hit the two decode failures, invalid JSON and invalid UTF-8.
  A cut JSON object can never parse, because its closing brace is the last byte, and 0xFF
  never occurs in valid UTF-8.
- `huge-counter` sends `10**400` as the uptime: valid JSON, too large for a float, so
  `math.isfinite` raises `OverflowError` — rejected as not-finite rather than crashing.
- `rtc-reset` sends a timestamp with no time zone, as an RTC chip has none. The validator
  treats it as UTC before judging its 26-year skew; without that branch the subtraction would
  raise `TypeError`.
- `firmware-v2` is the benign one: the unknown `fan_rpm` field is ignored, and the valid
  `+08:00` timestamp is normalised to UTC.
- `number-as-text`, `nan-reading` and `boolean-reading` land inside the CPU-usage, GPU and
  memory episodes, which shows that a rejected reading leaves an episode intact.
  `adc-saturated` lands as the CPU heats up, and its 255 °C is rejected by validation, so it
  never becomes a `peak_value`: bad data is not an anomaly.

### Same seed, same bytes

Each frame depends only on the seed, the frame number and the send time. Noise and faults
draw from two generators seeded per frame, so the send time reaches no reading: only the
`timestamp` field and, through the payload's length, the bytes where `truncated` and
`bit-flip` cut. The same `SIM_SEED` and settings therefore give the same readings in every
run, and byte-identical payloads whenever the send time is the same too — in a dry run
with `--start`; live frames carry the clock's time. Any frame can be computed on its own,
and switching faults off (`SIM_FAULT_EVERY=0`) leaves every clean frame unchanged. Another
seed moves threshold crossings by a second or two and puts each catalogue entry at a
different spot in its block; the timeline above holds for all of them.

### Narration

`docker compose logs -f simulator` reads as a running commentary. Seed 7's first 34 seconds,
with the log prefixes removed:

```text
tick 0: gateway powered on, uptime starts at 0
tick 0: idle: background load only
tick 9: fault sensor-timeout (GPU sensor did not answer) -> expect gpu_temperature_c:missing
tick 10: fault truncated (radio dropped the tail of the packet) -> expect payload:undecodable
tick 28: fault null-reading (fuel gauge timed out on the I2C bus) -> expect battery_percent:null
tick 30: video-analytics batch starts: CPU, memory and network climb
tick 33: fault rtc-reset (RTC backup cell is flat, the clock restarted at 2000-01-01 with no time zone) -> expect timestamp:receipt-implausible
```

The API folds both kinds of timestamp substitution into one counter; the narration is where
`rtc-reset`, `epoch-millis` and `http-date` can be told apart.

### Dry run

The simulator also runs without a broker, writing frames as JSON Lines. It needs Python
3.11 or later but not paho; run it from the repository root (on Windows the interpreter is
`python`, not `python3`):

```bash
python3 -m simulator --count 20
```

Narration goes to stderr, so stdout carries only data. With `--start` the output is
reproducible byte for byte, and `--out` writes the file as raw bytes, which matters because
the `bit-flip` line is deliberately not UTF-8 and a PowerShell redirect would re-encode it:

```bash
python3 -m simulator --count 240 --start 2026-10-19T08:00:00+00:00 --out day.jsonl
```

Line 58 of `day.jsonl` is frame 57, mid-batch:

```json
{"device_id": "gateway-01", "sequence": 57, "timestamp": "2026-10-19T08:00:57+00:00", "cpu_usage_percent": 97.64, "cpu_temperature_c": 86.66, "gpu_temperature_c": 69.04, "memory_used_percent": 93.91, "network_rx_kbps": 16081.4, "network_tx_kbps": 3196.39, "disk_usage_percent": 89.64, "battery_percent": 31.73, "memory_total_mb": 4096, "memory_used_mb": 3846.6, "uptime_seconds": 57}
```

In live mode the timestamps come from the clock and carry microseconds
(`2026-10-19T08:00:57.123456+00:00`); the service normalises them to `Z`. `sequence` is the
frame number and never resets; the service does not use it.

### Why it is built this way

- **Keyframes joined by straight lines, counted in messages.** The story is a table of
  `(message, value)` points per metric, plus a little uniform noise. The anomaly rules count
  consecutive messages, so a story written in messages keeps its margins at any
  `SIM_RATE_HZ`: changing the rate only fast-forwards or slows it, and uptime is converted
  back to real seconds.
- **Slope beats noise wherever a threshold is crossed.** On those stretches the change per
  message is more than twice the noise amplitude, so readings move strictly one way and each
  threshold is crossed once. With more noise than slope, one event would flicker into
  several, and the debounce would quietly drop the fragments. Disk and battery move by
  hundredths of a point per message and their rules fire on a single sample, so they carry
  no noise at all.
- **Only `publisher.py` imports paho.** Everything else in the package is pure functions,
  testable without a broker — the same split as the service, where only `ingest.py` knows
  paho exists. The dry run never imports the publisher.

### Settings

The MQTT variables are the service's own, so one `.env` serves both containers; the
simulator's own knobs carry a `SIM_` prefix. A bad value fails at startup with a
`ValueError`, not at message 300.

| Variable | Default | Meaning |
|---|---|---|
| `MQTT_HOST` | `localhost` | compose sets `broker` |
| `MQTT_PORT` | `1883` | |
| `MQTT_USERNAME` | unset | unset or empty: connect anonymously |
| `MQTT_PASSWORD` | unset | needs `MQTT_USERNAME`; a password alone fails at startup |
| `MQTT_TOPIC` | `devices/gateway-01/telemetry` | must match the service's |
| `SIM_DEVICE_ID` | `gateway-01` | must not be empty |
| `SIM_RATE_HZ` | `1` | 0.1–10; a higher rate fast-forwards the story |
| `SIM_SEED` | `7` | any integer; same seed and settings, same readings (same bytes in a dry run with `--start`) |
| `SIM_FAULT_EVERY` | `10` | one catalogue entry per N messages; `0` turns injection off, `1` is rejected |
| `LOG_LEVEL` | `INFO` | |

Compose passes `SIM_SEED` and `SIM_FAULT_EVERY` through from the shell or `.env`, so
`SIM_SEED=11 docker compose up -d simulator` replays the story with different noise and
different fault positions.

## Design decisions

**FastAPI and uvicorn, with sync handlers.** Five read-only `GET` endpoints, one query
parameter, no request bodies, no auth — almost any framework would do. FastAPI is chosen
for three things actually used here: query-parameter validation from a type annotation, so
`?component=tpu` returns a 422 naming the allowed values with **zero** validation code of
our own; a generated `/docs` page, which is a working test console for anyone trying the
service; and dict-to-JSON serialisation, so handlers return the pipeline's output directly.
Handlers are sync `def` rather than `async def` because they take the store's blocking
lock — a blocking acquire inside a coroutine stalls the whole event loop, whereas FastAPI
dispatches sync handlers to a threadpool where a blocked worker affects only itself.
Nothing under `src/telemetry/` imports a web framework at all, so the pipeline is testable
without one.

**Validation is a whitelist, not a blocklist.** A reading is kept only if it proves to
be a finite number of numeric type inside a plausible range. Firmware keeps finding new ways
to be wrong, so any list of known-bad shapes is one fault behind. `validation.py` names none
of the simulator's fifteen faults, yet each one meets its documented verdict through the
same few checks. A fault nobody has seen takes the same path as one we have.

**Validation is per-field, not per-message.** One number sent as text should not discard
the ten good readings beside it.

**Two thresholds, two owners.** The validator encodes *physical impossibility*
(CPU −20…120 °C); the anomaly detector encodes *operational concern* (CPU > 85 °C). A
91 °C reading at the peak of a batch job is real — it passes validation and is reported as
an anomaly. A 255 °C reading from a saturated 8-bit ADC is corrupt — it is quarantined and
never reaches the statistics. Merging them would mean widening an alert silently changes
what data is kept.

**`bool` and non-finite floats are rejected explicitly.** `isinstance(True, int)` is
`True` in Python, so a naive numeric check accepts `true` and averages it in as `1.0`
with no error; `json` accepts `NaN` by default, and `NaN` compares `False` against
everything, breaking min/max silently. The simulator sends both.

**Cross-field consistency is checked, not just field validity.** A device clock can be
wrong while its output is perfectly well formed. When a gateway's RTC backup cell goes
flat, the clock restarts at 2000-01-01 and the device reports timestamps such as
`2000-01-01T00:00:33`: valid ISO-8601 that passes every field-level check and is only
detectable against our own receipt time. Faults like this are found by *using* the data
rather than by inspecting more of it — left in, this one would surface as an anomaly episode
whose handful of 1 Hz samples spans 26 years. So a timestamp more than 60 s from the
receipt time is replaced by it, and the substitution is counted and labelled. Memory is
cross-checked too: used megabytes that disagree with the reported percentage by more than
1.5 points raise a flag, and so does an uptime that goes backwards.

**Rejected data is counted and kept, never silently dropped and never interpolated.**
Per-field rejection counts by reason are exposed under `data_quality`, with recent raw
values retained verbatim. Unknown faults cannot be enumerated, but the rate at which our
assumptions are violated can be measured — a field's rejection rate moving is the signal,
no prediction required. Rejected fields are simply absent for that second; filling them
from neighbours would mix synthetic values into the statistics irreversibly.

**Storage is in-process memory, with two retention strategies.** All-time
count/min/max/mean/stdev come from Welford running aggregates: exact, O(1) memory,
independent of uptime. Median and p95 need real samples, so they come from a window of the
last 3600 messages (an hour at 1 Hz, about 1 KB each) and are labelled as a window rather
than presented as all-time. A value folded into a running aggregate can never be removed,
which makes validation quality the ceiling on statistics quality; reporting both groups
side by side also means a disagreement between them exposes historical contamination.

**All-time temperature extremes are tracked separately from the window, without a
heap.** `top-temperatures` reports the all-time 3 highest and lowest, so it cannot read
from the 1-hour window — an extreme from two hours ago would silently age out. A dedicated
tracker keeps 6 readings per metric, O(1) memory, immune to eviction. Insertion compares
against the current third place of each list — two comparisons — and returns before
allocating anything when the value qualifies for neither, which is almost every reading:
over an hour of the simulator's stream, 98% of CPU temperature readings change neither
list. Speed is not the reason for the
design, though — at 1 Hz any approach is free. The reason to prefer it is that highest and
lowest share one code path by flipping a comparison, where a heap needs value negation for
the lower half, and the list stays sorted so reading the answer out is free. This does not
generalise: at k in the hundreds a heap's O(log k) wins and the negation becomes worth
paying. Note also that this endpoint is a pure maximum and so the most exposed to bad
data — one corrupt 255 °C reading would hold the top slot permanently, and more samples
make that more likely rather than less, which is why the tracker is fed only from
validated readings.

**Anomaly thresholds are fixed per metric, not learned.** A disk that fills slowly and then
stays full looks perfectly steady. A rolling mean ± k·σ detector adopts that as its
baseline and goes silent on the metric that matters most — the simulated disk creeps up by
0.02 points a message, which no learned baseline would call unusual. Thresholds are set
from what is operationally acceptable for the hardware and deliberately not fitted to
observed data, because the observed distribution may well be the distribution of an
unhealthy device.

**Anomalies are reported as episodes, not points.** At 1 Hz, ten minutes of overheating
is 600 violating samples. A run collapses into one record with a start, an end, an
`occurrences` count, a peak, and an `ongoing` flag — the last being the most useful field
an operator has. One cycle of the simulator's story (seed 7) yields six episodes where
point reporting would produce 367 records even after the debounce.

**`system-status` returns the freshest valid value per field, aged on a monotonic
clock.** The caller wants to know whether the device is healthy, not whether the last
packet was well formed, so one bad field does not blank the rest. The cost is a record
stitched from several instants, so every field carries its own `as_of`, and a field whose
newest valid value is over 30 seconds old is reported as `null` with `"stale": true`.
Age is measured against our own receipt time using `time.monotonic()` — not against the
payload timestamp, which the device can get wrong (three catalogue entries do), and not
against the newest message's timestamp, which freezes when the stream dies and would mean
nothing ever goes stale.

**One lock and one `try/except`.** Ingestion runs on paho's background thread while HTTP
handlers read, so the store is guarded by a single `threading.Lock` (measured 72 ns per
acquisition, uncontended at 1 msg/s). The only broad `except Exception` sits at the MQTT
callback: an exception escaping it would stop the network loop while HTTP kept answering
from a store that no longer updates, so the service would pass health checks while
serving frozen data. What it catches is logged with the raw payload and counted in the
ingestor's `handler_errors` counter (not part of the API), which the smoke test requires to
stay at zero: a non-zero count is a path validation does not cover. Two existed — an
integer literal past Python's 4300-digit parsing limit, and JSON nested deeper than the
decoder recurses — and both escaped `validate_payload`; both are now counted as undecodable
payloads.

## Packaging

The service image is a single-stage `python:3.11-slim`. Three things were considered and
rejected, each because it solves a problem this project does not have:

- **Multi-stage build** — its purpose is keeping compilers out of the runtime image, and
  nothing here compiles. `paho-mqtt`, `fastapi`, `uvicorn` and `pydantic` all resolve to
  `py3-none-any` wheels and `pydantic-core` ships prebuilt `manylinux` wheels for x86_64
  and aarch64. A second stage would save only pip's cache, which `--no-cache-dir` handles.
- **Alpine base** — musl libc means many Python packages have no wheel and compile from
  source, reintroducing the toolchain. The ~40 MB is not worth it.
- **`tini` / `--init`** — reaps zombie child processes; this service spawns threads, not
  processes.

`CMD` uses the exec form (JSON array), which is not cosmetic: it starts the app as PID 1
with no shell in between, so `docker stop`'s SIGTERM reaches uvicorn directly and the
shutdown hook runs. Shell form wraps the command in `/bin/sh -c`, and Docker's Dockerfile
reference warns that the shell does not pass signals on — the app is then killed when the
stop timeout runs out instead of shutting down.

**No `--platform` pin anywhere.** `python:3.11-slim` and `eclipse-mosquitto` are both
multi-platform images (OCI image indexes), so `docker compose` picks the host's platform
automatically and whoever runs it gets native images, Apple Silicon included. Pinning would push everyone whose machine differs from the author's into
emulation.

**The simulator has its own image.** It stands in for a gateway that real hardware would
replace, so it does not belong in the service's image, and sharing one would mean either
moving it into `src/` or shipping FastAPI and uvicorn to the "device". The cost is that
`paho-mqtt` is pinned in two requirements files, and a unit test checks that the pins
agree. Its Dockerfile sets `STOPSIGNAL SIGINT`: Python turns SIGINT into
`KeyboardInterrupt`, the publisher disconnects cleanly and `docker stop` returns at once,
where a SIGTERM to a PID 1 with no handler for it would be ignored until Docker kills the
container ten seconds later.

**Readiness, not liveness.** The service's compose healthcheck passes once `/health`
reports `broker_connected: true`. `/health` itself always answers 200 — "the process is
alive" and "it has a source" are different questions — so the check is a readiness gate:
`--wait` and the simulator's `depends_on: condition: service_healthy` use it, which makes
0:00 of the story a moment when the service is already subscribed. Plain Docker never
restarts a container for being unhealthy, and the check allows 12 failures five seconds
apart, so a broker outage longer than a minute marks the service unhealthy and nothing
restarts it. It does not use `messages_received > 0`, which would mark the service
unhealthy whenever the simulator stops. The image has no curl, so the check is one line of
Python.

The containers share compose's project network and reach the broker by its service name,
`broker`, rather than through `host.docker.internal`, which does not exist on stock Linux
Docker and would work on the author's laptop while failing on the next machine.

## Testing

Needs Python 3.11 or later. Where `python3` is older, name a newer interpreter instead, for
example `python3.11 -m venv .venv`.

```bash
python3 -m venv .venv
```

```bash
./.venv/bin/pip install -r requirements-dev.txt
```

```bash
./.venv/bin/python -m pytest -q
```

(On Windows the interpreter is `python`, not `python3`, and the virtual environment's
programs are under `.venv/Scripts/`.)

**Every test but the smoke test runs without a broker.** Unit tests cover each module on
its own: `on_message` takes an object with a `.payload` attribute, so a three-line fake
stands in for paho's message. The simulator's story, rotation, settings, dry run and
publisher are unit-tested the same way in `tests/test_simulator.py`, with a stand-in for
paho's client.

**The contract test.** `tests/test_simulator_contract.py` is the one place that imports
both programs. It runs the real validator, store and API against the simulator's promises:
every catalogue entry against its documented verdict; every frame of two full cycles, for
ten seeds, against its label — stricter than comparing totals, since two verdicts swapped
between frames would leave every total intact; one cycle through a real store, asserting
one closed episode per rule (which also proves the backup burst was dropped) and
data-quality counts derived from the catalogue rather than written as literals; the moment
all six rules are ongoing; and every endpoint through FastAPI's test client, including the
503 and staleness. Change either side and this file names the promise that broke.

One end-to-end smoke test (`tests/test_smoke.py`) drives the real ingestor against the
compose broker and simulator. It skips itself when nothing is listening on port 1883, so
on a machine with no Docker pytest reports it as skipped and everything else passes. To
run it:

```bash
docker compose up -d --build --wait
```

```bash
./.venv/bin/python -m pytest -m smoke -q
```

`validation.py` is additionally mutation-tested with `mutmut`, against
`tests/test_validation.py` and the contract test. mutmut needs Linux or macOS (a
`python:3.11-slim` container works too), not native Windows:

```bash
./.venv/bin/mutmut run        # configured in setup.cfg
```

Coverage answers "did this line execute"; mutation answers "if this line were wrong, would
a test fail". A suite can reach 100% coverage while asserting almost nothing. That module
carries the project's central claim — that a positive whitelist rejects fault shapes nobody
has seen — so its test quality is worth checking rather than assuming. The scope is
deliberately that one module: it is pure, with no I/O, clock or threads, so the run takes
about a second per full suite pass.

Mutation testing earned its keep in earlier runs. It is what identified
`value.replace("Z", "+00:00")` as dead code — two mutants of that call survived, which is
the signature of a line that cannot affect behaviour, and `datetime.fromisoformat` has
parsed a trailing `Z` natively since 3.11. It also caught a test asserting
`len(raw) == MAX_RAW_REPR`, which compares the code against its own constant and therefore
verifies nothing.

## Load

With the stack up (on Windows the interpreter is `python`, not `python3`):

```bash
python3 scripts/stress_test.py --duration 30 --concurrency 8
```

Standard library only, so it runs without installing anything. It rotates through all five
endpoints from several workers and reports per-endpoint latency, but the check that matters
is the ingestion one: **every endpoint takes the same non-reentrant lock the MQTT ingest
thread needs**, so a read-heavy load could in principle starve the writer and make the
service drop telemetry while still answering 200 to everything — invisible from the HTTP
side. The script therefore compares `messages_received` before and after against the
publisher's 1 Hz.

Measured on a 12-core macOS host against an earlier build of the service, with a 1 Hz
publisher. The request path (handlers, store, statistics) has changed only in comments and
wording since, but these figures were not re-measured on the compose stack:

| workers | throughput | `statistics` p50 | other endpoints p50 | errors | ingestion |
|---:|---:|---:|---:|---:|---:|
| 8 | 2450 req/s | 3.8 ms | 2–3 ms | 0 | 1.00/s |
| 32 | 1799 req/s | 64.2 ms | 6–12 ms | 0 | 1.00/s |

Ingestion held at exactly the publisher's rate in both runs — the lock did not starve the
writer. Throughput *fell* as workers rose, which is oversubscription rather than distress:
in those runs the container sat at ~108% CPU on a 12-core machine, so the ceiling is one
core.

`/api/v1/statistics` is the only endpoint that touches the window, sorting each metric's
samples per request. Measured in-process on that build: 0.21 ms at 300 samples, 2.30 ms at
the full 3600; the others stayed under 0.1 ms. Its 64 ms under 32 workers was almost
entirely queueing.

## Configuration

The service reads these at startup:

| Variable | Default |
|---|---|
| `MQTT_HOST` | `localhost` (compose sets `broker`) |
| `MQTT_PORT` | `1883` |
| `MQTT_USERNAME` | unset: connect anonymously |
| `MQTT_PASSWORD` | unset; needs `MQTT_USERNAME` |
| `MQTT_TOPIC` | `devices/gateway-01/telemetry` |
| `API_PORT` | `8000` |
| `API_HOST` | `0.0.0.0` |
| `LOG_LEVEL` | `INFO` |
| `HISTORY_WINDOW` | `3600` samples |
| `MAX_CLOCK_SKEW_SECONDS` | `60` |
| `STALE_AFTER_SECONDS` | `30` |

Broker credentials are optional, and anonymous is the default: the demo broker accepts
anonymous clients and is bound to `127.0.0.1`. To require a login, set
`allow_anonymous false` and a `password_file` in `mosquitto/mosquitto.conf` (the file shows
how), mount the password file in the broker's `volumes` in `compose.yaml` (only
`mosquitto.conf` is mounted today), and put `MQTT_USERNAME` and `MQTT_PASSWORD` in a `.env`
file next to `compose.yaml`. Compose reads `.env` on its own and hands both values to the
service and the simulator. `.env` is gitignored and never committed. An empty value counts
as unset, and a password without a username fails at startup rather than being silently
dropped.

## Limitations

- The simulator is a demo device, not a statistical model: four-minute cycles, a catalogue
  entry every ten seconds in a fixed order, and time compressed so that a battery drains in
  minutes. It exists to drive every path of the service on cue, not to resemble a real
  fleet's fault rates.
- Changing `SIM_FAULT_EVERY` or `SIM_RATE_HZ` moves the timeline: which entry lands when,
  the per-cycle counts, and the guarantee that the backup burst never meets a CPU-usage
  fault all assume the defaults. The tests pin the defaults, and across seeds they assert
  only what holds for every seed.
- Timestamps come from the simulator's clock and are judged against the service's clock at
  receipt. In compose both containers read the same kernel clock (the virtual machine's, on
  Docker Desktop), so they agree to within milliseconds whatever that clock does: if it
  drifts from the host's after a laptop sleeps, both sides move together and the
  60-second skew check never fires. That agreement is the only reason the threshold is
  defensible; a distributed fleet would need a different bound. The caveat applies to a
  consumer running outside Docker, such as the service started from the virtual
  environment or the smoke test's in-process ingestor: it reads the host's clock while the
  simulator reads the virtual machine's, and once those differ by more than 60 s every
  timestamp is replaced by the receipt time. Restarting Docker Desktop fixes that.
- Restarting the simulator while the service runs restarts its frame counter and uptime:
  the service counts one more `uptime_regressed` and the story starts over. That is correct
  — to the service, the device rebooted.
- The two kinds of timestamp substitution share one `timestamp_substituted` counter, and
  `system-status` shows `timestamp_source` only until the next good message. The
  simulator's narration tells them apart.
- Each cycle closes six episodes, 90 an hour, so the 1000-episode history fills after about
  11 hours. From then on the oldest are dropped and `episodes_discarded` counts them; a cap
  that truncated silently would read as complete coverage.
- Both sides default to the topic `devices/gateway-01/telemetry`. Changing `SIM_DEVICE_ID`
  does not change the topic, and compose passes only the credentials, `SIM_SEED` and
  `SIM_FAULT_EVERY` through from `.env`: to rename the device, add `SIM_DEVICE_ID` and a
  matching `MQTT_TOPIC` to the simulator's `environment` in compose.yaml, and the same
  `MQTT_TOPIC` to the service's.
- The broker accepts anonymous clients and is bound to 127.0.0.1. Anywhere else, give it a
  password file and set credentials on both sides; the code already supports that.
- `sequence` is advisory and nothing is deduplicated. Counters restart when a device
  reboots, so a repeated sequence number does not prove a repeated message, and a gap does
  not prove a loss.
- Statistics accumulate from service start and reset when the process exits — not only on
  power loss but on restart, redeploy or OOM kill. Acceptable for a demo whose device
  replays the same story every four minutes; SQLite would be a drop-in replacement behind
  the same interface if persistence were required.
- **Throughput is capped at roughly one CPU core, and the service cannot be scaled with
  `uvicorn --workers N`.** In the load run above the container sat at ~108% CPU on a
  12-core host: one Python process, one GIL. Adding uvicorn workers would not help and would actively
  break correctness — the store is in-process memory, so each worker would hold its own
  MQTT subscription and its own partial history, and they would disagree with one another.
  This is a property of the design rather than a tuning problem, and it is the right design
  for one device at 1 Hz: the 2450 req/s of that run is three orders of magnitude above what
  this service is for. Genuinely scaling it would mean moving the store out of the process,
  which is the same change as making it survive a restart.
- Dry-run files are not pure UTF-8: the `bit-flip` line contains 0xFF on purpose. Write
  them with `--out` and read them as bytes.

## License

MIT — see [LICENSE](LICENSE).
