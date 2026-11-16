# petbowl

A pet-bowl IoT telemetry pipeline, built to answer one question honestly: **what does it take to ingest
unreliable device data without losing, double-counting or silently misordering it?**

Fake devices emit weight, water, battery and heartbeat readings. A fault injector then makes the network
misbehave (duplicates, reordering, offline bursts, clock skew, corrupt payloads). An ingest API
absorbs all of that with an explicit ack contract. It comes in two shapes: **direct**, which writes to Postgres
and keeps per-device state in a hand-rolled WAL-backed KV store, and **queue**, which validates and publishes to
Kafka so storage can be decoupled from the request path. A small console lets you drive the simulator and watch
events move through every stage.

```mermaid
flowchart LR
    subgraph Sim["Simulator (python -m petbowl.simulator)"]
        D[Devices<br/>virtual time] --> F[Fault injector] --> P[Pacer] --> S[Sink]
    end
    S -- "NDJSON over HTTP<br/>batches up to 500" --> I["Ingest<br/>POST /v1/events"]
    I -. "ack: accepted / duplicates /<br/>rejected / gaps / late" .-> S

    I -- "direct mode<br/>(ingest.app)" --> PG[(Postgres<br/>events)]
    I -- "direct mode" --> KV[(KV + WAL<br/>per-device state)]

    I -- "queue mode<br/>(ingest.queue_app)" --> K[["Kafka<br/>petbowl.events<br/>key = device_id"]]
    K -. "consumers: next" .-> PG
    K -. "consumers: next" .-> KV

    C[Console :8200] -. "tap, probes, log tail" .-> I
    C -.-> K
    C -.-> PG
```

Status: simulator, both ingest modes, KV/WAL, Postgres storage, the Kafka publisher and the console are done.
Consumers (Kafka to Postgres and KV) are next (see [Roadmap](#roadmap)). Until they exist, queue mode accepts and
stores events in Kafka only.

---

## Quick start

Requires Python 3.13+, [uv](https://docs.astral.sh/uv/) and Docker (for Postgres).

```bash
make install        # uv sync
make db-up          # Postgres 17 on 127.0.0.1:5432 (waits until healthy)
make serve          # ingest API on :8100, single worker

# in another terminal
uv run python -m petbowl.simulator --devices 200 --hours 20 --seed 1 --speedup 3600 \
  --sink http://127.0.0.1:8100/v1/events \
  --dead-letter /tmp/dead.jsonl --ack-log /tmp/acks.txt \
  --malformed-rate 0.01 --dup-rate 0.02

# in a third terminal, to watch it
uv run python -m petbowl.console    # http://127.0.0.1:8200
```

The schema is applied automatically when the API starts. `make ci` runs lint, mypy (strict) and the tests.

**Queue mode** (Kafka instead of Postgres/KV on the request path):

```bash
make kafka-up                                   # single-node KRaft broker on 127.0.0.1:9092
make serve-queue                                # ingest.queue_app on :8100 (WORKERS=4 make serve-queue to scale)
uv run python -m petbowl.simulator --devices 200 --hours 20 --speedup 3600 \
  --sink http://127.0.0.1:8100/v1/events        # same simulator, same URL
uv run python -m petbowl.console --mode queue   # console with Kafka in the pipeline
```

Run one ingest at a time on `:8100`: `make serve` (direct) or `make serve-queue` (queue).

---

## The simulator

`python -m petbowl.simulator` is a lazy pipeline, so memory stays flat no matter how long the virtual run is:

```
make_fleet -> stream -> apply_faults -> pace -> sink.write(batch)
```

| Stage | What it does |
|---|---|
| **Devices** (`device.py`) | Each device is a seeded state machine ticking every 30 s ± 2 s in *virtual* time. It emits `weight_g` each tick, `heartbeat` every 2 ticks, and `battery_pct` and `water_ml` every 20 ticks. It also models meals, refills and battery drain. `stream()` merges all devices in time order with a heap. |
| **Faults** (`faults.py`) | Applied in this order: clock skew, malformed payload, offline burst, out-of-order delay, duplicate. Each is a rate or magnitude on the CLI. Every fault exercises one class of ingest behaviour. |
| **Pacer** (`pace.py`) | Groups deliveries into batches (up to 500 items, 20 ms window) and sleeps until each is due. `--speedup N` plays N virtual seconds per wall second; `0` is unpaced. |
| **Sinks** (`sinks.py`, `http_sink.py`) | `null`, `stdout`, `jsonl:<path>` or an `http(s)://` URL. |

Runs are deterministic for a given `--seed`, so a failing scenario can be replayed exactly.

**HTTP sink behaviour.** It is a real client, not a firehose. It retries on network errors and 5xx with
jittered exponential backoff and honours `Retry-After`. It splits a batch in half on 413. On 400/413 for a
single item, or after the final retry, it writes the item to `--dead-letter` as `{reason, payload}`.
Delivered events go to `--ack-log` as `device_id:seq`. Both files are append-only and accumulate across runs.

| Flag | Default | Meaning |
|---|---|---|
| `--devices`, `--hours`, `--seed` | 100, 24, 42 | fleet size, virtual duration, RNG seed |
| `--speedup` | 0 | virtual seconds per wall second (0 = as fast as possible) |
| `--sink` | `null` | `null`, `stdout`, `jsonl:<path>`, or ingest URL |
| `--dup-rate`, `--ooo-rate`, `--offline-rate`, `--malformed-rate` | 0 | fault probabilities |
| `--skew-s` | 0 | device clock skew magnitude, in seconds |
| `--dead-letter`, `--ack-log` | off | files for items that failed or landed |

---

## Event contract

One JSON object per line (NDJSON):

```json
{"device_id": "dev-00042", "seq": 1187, "ts": "2026-01-01T09:14:03.210Z", "kind": "weight_g", "value": 312.4}
```

| Field | Notes |
|---|---|
| `device_id` | 1 to 64 characters |
| `seq` | monotonic per device, ≥ 0 |
| `ts` | device clock, **untrusted**, must carry a UTC offset |
| `kind` | `weight_g`, `water_ml`, `battery_pct` or `heartbeat` |
| `value` | float, or null |

`event_id = device_id:seq` is the **idempotency key**. It is derived from the event, never random, so any
retry or duplicate is recognisable. Ingest stamps its own `received_at`, so both "when it happened" and "when
we learned of it" are kept. The model is frozen and rejects unknown fields (`common/events.py`).

---

## Ingest

`POST /v1/events` takes an NDJSON body and answers per batch:

```json
{"accepted": 480, "duplicates": 12, "rejected": 8, "gaps": 3, "late": 5,
 "rejects": [{"index": 17, "reason": "kind: Input should be 'weight_g', ..."}]}
```

| Outcome | Meaning |
|---|---|
| `accepted` | new event, stored |
| `duplicates` | `device_id:seq` already stored, no-op |
| `rejected` | failed validation. `rejects[]` gives the line index and reason (capped at 100 per response) |
| `gaps` / `late` | accepted events classified against device state: a jump in `seq`, or `seq < max_seq` |

Other statuses: `400` empty batch, `413` over 5 MB or 5000 lines, `503` + `Retry-After: 1` when Postgres is
unavailable. `GET /healthz` checks the database, and `GET /v1/devices/{id}/state` returns `{max_seq, max_ts}`.

Per-batch flow inside `service.process_batch`. It has no HTTP or driver imports and works against two small
protocols, `EventStore` and `StateStore`:

```
HTTP request (NDJSON body, N lines)
      |
      v
   app            splits lines, calls process_batch(lines, PgEvents(pool), kv)
      |
      v
  service.process_batch
      |  1. parse each line -> Event, or REJECTED(reason)
      |
      |  2. events.insert(valid events) ------------> PgEvents.insert
      |                                                 |  pool gives a connection
      |                                                 |  BEGIN
      |                                                 |  INSERT ... unnest(...) ON CONFLICT DO NOTHING
      |                                                 |  Postgres returns only the rows it inserted
      |                                                 |  COMMIT (Postgres fsyncs its own WAL)
      |  <-- [True, False, True, ...] ----------------- |  insert_events turns that into one bool per event
      |
      |  3. per event: state.get(device) -> classify (gap / late / ok), advance in memory
      |
      |  4. per changed device: state.put(device, new state) ----> KV: append to WAL, then dict
      v
  BatchResult (outcome + detail per line) -> app turns it into the JSON response
```

### Why the order is Postgres first, KV second

Postgres is the source of truth. KV state (`max_seq`, `max_ts` per device) is **derived** and can always be
rebuilt. A crash can then only leave KV *behind* Postgres, never ahead. On startup, `repair_state` runs
`SELECT device_id, max(seq), max(ts) FROM events GROUP BY device_id` and raises each KV entry to at least that
value (a max-merge, so it is idempotent). If KV were written first, a crash would leave state claiming events
that were never stored, which no query could repair.

### Idempotency in one statement

`INSERT ... SELECT * FROM unnest(...) ON CONFLICT (device_id, seq) DO NOTHING RETURNING device_id, seq` makes
the primary key the arbiter. The returned rows say which events were new, so a whole batch is one round trip
and one transaction. A repeat inside the same batch also counts as a duplicate.

### Queue mode (`ingest/queue_app.py`)

Same endpoint, same framing and same ack shape, but the handler's job shrinks to **validate and hand off**:

```
HTTP request (NDJSON body)
      |
      v
  validate_lines            Event.model_validate_json per line -> valid | REJECTED(reason)
      |
      |  valid lines only, key = device_id, value = the raw line as received
      v
  Publisher.publish(topic, items)       returns only when Kafka has acked every item (acks=all)
      |
      v
  200 {accepted: <valid>, rejected: <bad>, duplicates: 0, gaps: 0, late: 0, rejects[]}
  503 + Retry-After: 1 if the publish fails (the client retries the whole batch)
```

What this changes, and what it does not:

| | Direct (`ingest.app`) | Queue (`ingest.queue_app`) |
|---|---|---|
| Durable before the 200 | Postgres commit | Kafka ack from the broker (`acks=all`, idempotent producer) |
| `duplicates` / `gaps` / `late` | computed | always 0: no state here, classification moves to the consumer |
| `accepted` means | stored in Postgres | **accepted for processing**, not yet queryable |
| Scale-out | one worker (KV `flock`) | stateless, so `WORKERS=N make serve-queue` works |
| `/healthz` | checks Postgres | process liveness only, it does not check Kafka |
| Per-device ordering | transaction order | all of a device's events share one partition (key = `device_id`) |

Delivery is **at least once**. A batch that fails midway may leave some items already in Kafka, and the client
retries the whole batch, so the same `device_id:seq` can appear twice. This is deliberate and safe because
every downstream write is idempotent (`ON CONFLICT (device_id, seq) DO NOTHING`, ADR-001). The `Publisher`
contract in `mq/base.py` states it explicitly: raise `PublishError`, callers retry the whole batch.

---

## Message queue (`mq/`)

A thin, typed seam between the app and the broker, so nothing above it imports a Kafka client:

```python
class Publisher(Protocol):
    async def publish(self, topic, items: Sequence[tuple[bytes, bytes]]) -> None  # durable on return, else PublishError
    async def close(self) -> None

class Consumer(Protocol):
    def poll(self, max_messages, timeout_s) -> list[Message]    # Message = (topic, partition, offset, key, value)
    def commit(self, messages) -> None                          # those messages and everything before them
    def close(self) -> None                                     # uncommitted messages are redelivered
```

| Implementation | File | Use |
|---|---|---|
| `KafkaPublisher` | `mq/kafka.py` | `confluent-kafka` producer: idempotent, `acks=all`, 5 ms linger, 10 s message timeout. A background thread drives delivery callbacks, which resolve per-item futures, so `publish()` returns when the broker has acked every item. |
| `KafkaConsumer` | `mq/kafka.py` | Group member with **manual** commits (`enable.auto.commit=false`, `auto.offset.reset=earliest`). Commit is synchronous, and the offset is the highest seen plus one per partition (`next_offsets`). |
| `create_topic` | `mq/kafka.py` | Idempotent topic creation. The broker has auto-create off, so the app creates `petbowl.events` (3 partitions, replication factor 1) at startup. |
| `MemoryBroker` + publisher/consumer | `mq/memory.py` | In-RAM partitioned logs and committed offsets, same protocols, with an injectable publish failure. Used by tests so the queue path runs without a broker. |

The commit contract is the important part for the next stage: a consumer must **write to Postgres first, then
commit**, so a crash re-delivers instead of losing data, and the idempotent insert absorbs the re-delivery.

The broker is `apache/kafka:4.0.0` in KRaft mode (no ZooKeeper), bound to `127.0.0.1:9092`, data in the
`kafkadata` volume (`make kafka-up`, `kafka-down`, `kafka-logs`).

---

## Storage

### Postgres (`ingest/schema.sql`)

```sql
events(device_id text, seq bigint, ts timestamptz, kind text CHECK (kind IN (...4 kinds...)),
       value float8, received_at timestamptz DEFAULT now(),
       PRIMARY KEY (device_id, seq))
```

### KV with a write-ahead log (`kv/`)

A small, purpose-built store: an in-memory dict made durable by an append-only WAL. It exists to keep
per-device state fast to read and cheap to write, and to make crash behaviour something we can test.

- **Write path:** `put` appends a record to the WAL, then updates the dict.
- **Record format** (`record.py`, [ADR-002](docs/ADR-002.md)): crc32 over `body_len + body`, so a damaged length is caught. A zero-filled tail can never decode as a valid record, and bodies are capped at 16 MiB.
- **Recovery** (`wal.py`): replay on open, and truncate at the first corrupt or torn record.
- **Durability policy:** `always` fsyncs every write, `interval` fsyncs on a timer (default 0.1 s), `never` leaves it to the OS. Fsync uses `F_FULLFSYNC` on macOS.
- **Single writer:** the store holds an exclusive `flock` on `<path>.lock`. Run ingest with **one worker**. Other processes must read state through `GET /v1/devices/{id}/state`, not the file.

Crash safety is tested by killing a writer mid-operation and reopening (`tests/test_kv_crash.py`).

### Configuration

| Env var | Default |
|---|---|
| `PETBOWL_DB_URL` | `postgresql://petbowl:petbowl@127.0.0.1:5432/petbowl` |
| `PETBOWL_KV_PATH` | `data/state.wal` |
| `PETBOWL_KV_POLICY` | `interval` |
| `PETBOWL_KV_INTERVAL_S` | `0.1` |
| `PETBOWL_KAFKA` | `127.0.0.1:9092` (queue mode, and the console's `--kafka`) |
| `PETBOWL_TOPIC` | `petbowl.events` |
| `PETBOWL_TOPIC_PARTITIONS` | `3` |

---

## Console

A lightweight, separate-process UI to drive the simulator and watch the pipeline. It lives in
`src/petbowl/console/` and does not import or patch simulator or ingest code. It observes at the edges:

```mermaid
flowchart LR
    UI[Browser UI] <-- "SSE + REST" --> CS[Console server :8200]
    CS -- "spawns, sink = /tap/ingest" --> SIM[Simulator]
    SIM -- "NDJSON" --> TAP[Tap proxy in console]
    TAP -- "forwards unchanged" --> ING[Ingest :8100]
    SIM -. "terminal run: --ack-log / --dead-letter files" .-> LOGS[(log files)]
    LOGS -. "tailed" .-> CS
    CS -. "GET /v1/devices/id/state<br/>only while a device is pending" .-> ING
    CS -. "read-only SQL" .-> PG[(Postgres)]
    CS -. "queue mode: watermarks, group lag, peek<br/>(no group, no commits)" .-> K[["Kafka"]]
    ING -- "queue mode" --> K
```

Two ways to see a run:

1. **Start it from the UI.** The console launches the simulator pointed at its tap, which forwards to ingest and publishes every batch and ack. This gives full detail: latency, status codes and reject reasons.
2. **Run the simulator from a terminal straight at `:8100`.** The console tails `--ack-log` (default `/tmp/acks.txt`) and `--dead-letter` (default `/tmp/dead.jsonl`). It gives counts and dead-letter reasons, but not latency or retries. It starts from the end of each file, so earlier runs are ignored.

**Two pipelines.** `--mode direct` (default) draws simulator, ingest, then Postgres and KV. `--mode queue`
(with `--kafka` and `--topic`, which default to the env vars above) draws simulator, ingest, **Kafka**, and
shows Postgres and KV hanging off Kafka with **dashed "planned" edges**, because the consumers that will feed
them don't exist yet. When a consumer is built it becomes a node and the dashed edges are replaced.

**Kafka metrics** (read-only: the probe never joins a group or commits, so it cannot disturb real consumers):

| Metric | Source |
|---|---|
| Broker reachable, topic exists | `list_topics` (a reachable broker with a missing topic shows as failed) |
| Per-partition low and high watermark, message count, total offsets | `get_watermark_offsets` |
| Produce rate and a 60 s sparkline, produced since console start | change in total end offsets, sampled every second |
| Consumer-group lag, total and per partition | admin `list_consumer_group_offsets` against the high watermarks, every 3 s. A partition with no commit yet counts as fully owed, because consumers start at `earliest`. |
| Last messages per partition | the Peek button: a throwaway assigned consumer reads the tail |

"Produced since console start" is an integrity check: it should equal the ingest node's `accepted` for the
same period. If the two diverge, an ack was returned for something the broker never stored.

**Macro and drill-down.** The home view is the pipeline graph: node health, a pulse on each edge per event.
Click a node for a type-specific panel (simulator form and log, ingest counters, Kafka metrics, KV per-device
landed or pending, Postgres rows and recent inserts), plus a filtered event tail. Click an event to highlight its
`device_id:seq` as it moves between nodes.

**Adding a component** (queue, consumer, …) is one plugin file plus topology entries:

| Piece | Where | Contract |
|---|---|---|
| Trace event | `console/bus.py` | `{node_id, node_type, kind, data, corr}`. The only shared structure. |
| Node plugin | `console/nodes/<type>.py` | `start(bus)`, `stop()`, `snapshot()`, optional `actions()` |
| Topology | `console/topology.py` | nodes and edges as data. The layout follows the edges. `Edge(planned=True)` draws a dashed, not-built-yet hop. |
| Widget | `console/static/widgets/<type>.js` | `summary`, `mount`, `update`, `onEvent`. Falls back to `generic.js`. |

```bash
uv run python -m petbowl.console --ingest http://127.0.0.1:8100 --port 8200
# also: --mode direct|queue, --kafka, --topic, --db-url, --ack-log, --dead-letter ('' disables a tail)
```

Notes: the tap adds one hop, so it is a dev tool and not part of the production path. The KV panel infers
"landed" by polling the ingest state route, and only for devices acked but not yet seen in KV. After wiping the
WAL, restart the console and truncate the log files so its view matches. In queue mode the KV node is idle,
since the queue ingest has no state route.

---

## Design decisions

The reasoning lives in `docs/`. The short version:

| Decision | Why |
|---|---|
| `event_id = device_id:seq`, derived not random ([ADR-001](docs/ADR-001-event-schema.md)) | Retries and duplicates become no-ops, not overwrites. Limit: seq resets after a reboot would collide, so a `boot_id` is needed first. |
| `ts` is untrusted, `received_at` is ours | Device clocks drift. `received_at - ts` is itself the skew and delay signal. |
| Gap vs late vs duplicate, per device | Gaps and late arrivals look identical at first, and only time tells them apart. A missing seq becomes loss only after a grace window. |
| Postgres first, KV derived, repair on boot | KV can lag Postgres but never lead it, and it is always rebuildable. |
| CRC covers length, zeros are never valid ([ADR-002](docs/ADR-002.md)) | Torn writes and zero-filled tails must not decode as data. |
| Explicit ack contract | The client knows exactly what landed, so it can retry, split or dead-letter. Nothing is dropped silently. |
| Queue mode acks only after Kafka acks (`acks=all`, idempotent producer) | A 200 means the event is durably queued. The cost is latency per request, and `accepted` stops meaning "queryable". |
| At-least-once delivery, idempotent sinks | Retrying a whole failed batch is simpler and safer than tracking partial publishes. `ON CONFLICT DO NOTHING` makes the re-delivery free. |
| Key = `device_id` | One device maps to one partition, so per-device order survives the queue and the consumer can classify gaps and late events. |
| Manual consumer commits, after the write | Commit-then-write would lose data on a crash. Write-then-commit re-delivers instead. |
| `Publisher` / `Consumer` protocols with an in-memory twin | Queue logic is tested without a broker, and the Kafka client stays swappable. |

---

## Performance

Measured on a MacBook Pro, Python 3.13, Postgres in Docker Desktop (`docs/BASELINE.md`):

| Config | items/s | request p50 | request p99 |
|---|---|---|---|
| Simulator alone, null sink (1000 devices, 24 h) | ~123,000 | n/a | n/a |
| End to end, defaults (KV interval, commit on) | 20.1k to 21.0k | 17.1 to 18.0 ms | 57.6 to 106.6 ms |
| KV fsync off | 21.7k to 22.1k | 16.6 to 17.2 ms | 46.4 to 66.3 ms |
| Postgres `synchronous_commit` off | 21.4k to 22.8k | 16.1 to 17.1 ms | 63.5 to 77.2 ms |

Takeaways. Neither fsync is the bottleneck: removing either gains about 6 to 7%, close to the 4% spread between
identical runs. The server peaks near 35% of one core, so one serial client measures **pipeline latency, not
capacity**. Real capacity needs parallel clients, which needs a `--device-offset` flag so concurrent simulators
use distinct device ids.

---

## Development

```bash
make ci          # ruff + ruff format --check, mypy --strict, pytest (with coverage)
make fmt         # auto-fix lint and format
make test        # just the tests
make kv-bench    # KV microbenchmark
make kafka-up    # also: kafka-down, kafka-logs
make serve-queue # queue-mode ingest (WORKERS=N to scale)
make db-psql     # psql shell into the dev database
make db-reset    # drop the volume and start Postgres fresh
```

- Tests use a throwaway Postgres schema per test (fixtures in `tests/conftest.py`). They skip when Postgres is down locally and fail in CI. CI runs against a `postgres:17` service.
- Property-based tests (Hypothesis) cover the fault injector and event model. Crash tests cover the WAL.
- Python 3.13, `uv` for dependencies, ruff line length 100.

### Repository layout

```
src/petbowl/
  common/events.py        Event model (pydantic, frozen)
  simulator/              devices, faults, pacing, sinks, HTTP client
  ingest/                 app.py (direct: Postgres+KV), queue_app.py (queue: Kafka), validate.py,
                          service (pure logic), db, repair, schema.sql, config
  kv/                     WAL-backed store: record format, WAL writer/recovery, store
  mq/                     Publisher/Consumer protocols, Kafka and in-memory implementations
  console/                visualizer: bus, topology, nodes/, static/ UI
docs/                     ADRs and the performance baseline
tests/                    unit, property, crash and integration tests
bench/                    KV benchmark
```

---

## Roadmap

- **Consumers**: Kafka to Postgres and KV, using the existing `Consumer` protocol. They write first and commit after, and carry the gap, late and duplicate classification that direct mode does inline. The console already draws these hops as planned edges and shows consumer-group lag the moment a group exists.
- Make queue-mode `/healthz` check Kafka, so a load balancer can see a broker outage.
- Gap grace-window handling, so a missing `seq` is declared lost only after the window expires.
- `boot_id` on events, so device reboots don't collide on `seq`.
- `--device-offset` on the simulator, for parallel-client capacity tests.
- Rollups bucketed by device `ts`, tolerant of late and out-of-order data.
