# ADR-001: Event schema

## 1. event_id = device_id:seq (idempotency key)
**Decision:** `event_id` is derived from the event's own fields, never random, and ingest treats it as the idempotency key.
**Why:** Networks retry and duplicate. Copies carry the same id, so ingest inserts if absent and drops the copy instead of double-counting.
**Consequence:** Retries are safe (duplicates are no-ops, not overwrites). Limit: a device whose `seq` restarts after a reboot would collide with old ids, so a `boot_id` is needed before reboots are supported.

## 2. ts is the device clock; received_at is added at ingest
**Decision:** `ts` is untrusted device time. Ingest stamps `received_at` (from stage 3).
**Why:** Real devices have skewed clocks and unreliable links, so when it happened and when we learned of it differ. We need both.
**Consequence:** Rollups bucket by `ts` and must accept late and out-of-order data. Time-based alerts need skew tolerance. `received_at - ts` per device is the delay and skew signal.

## 3. Classifying gaps, late events and duplicates
**Decision:** Per device, ingest keeps `max_seq` and recent seen ids. Seen id means duplicate. Unseen `seq < max_seq` means late arrival. A missing `seq` below max becomes loss only after a grace window.
**Why:** Gaps and late arrivals look identical at first, and only time tells them apart. Reboot detection is out of scope until `boot_id` exists.
**Consequence:** Needs durable per-device state, which is the KV store with WAL (step 2). The simulator's dup, out-of-order, offline and malformed faults each exercise one class.