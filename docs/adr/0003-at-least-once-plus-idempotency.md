# ADR 0003 - At-least-once delivery plus idempotent consumers

**Status:** accepted

## Context

Order intake writes to the database and publishes an event. A distributed
transaction spanning SQLite and the broker is not available (and would not be
used across service boundaries anyway). Exactly-once delivery is a myth without
cooperation from consumers.

## Decision

1. Broker guarantees **at-least-once**: offsets are committed only after the
   handler succeeds; nacked messages are redelivered with backoff; poison
   messages go to `<topic>.dlq` after `MAX_ATTEMPTS`.
2. Stock mutation is **exactly-once per idempotency key**: the storage node
   applies the value change and records `order:<id>` in the same SQLite
   transaction, returning the cached response for duplicates.
3. Order creation is **idempotent** through `Idempotency-Key` -> order-id pointer
   stored with optimistic concurrency.
4. Read-model projection is idempotent: the first terminal state wins.

## Consequences

* Duplicates are expected and harmless; they are counted
  (`inventory_duplicate_events_total`) instead of ignored.
* A crash between publishing and recording the publish marker can emit a
  duplicate event - safe by design, and documented.
* The dual-write window (order persisted, publish failed) is handled by returning
  `503` with `Retry-After` and letting the idempotent retry republish.
* A production system would add a transactional outbox for perfect atomicity;
  the trade-off is stated rather than hidden.
