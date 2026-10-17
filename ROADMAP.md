# Roadmap

Planned work for `rabbitmq-easy`, in the order it should be done.
Each heading is a release boundary: finish and ship one before starting the next.

Shipped changes are recorded in [CHANGELOG.md](CHANGELOG.md).

## 1.2.0 — efficiency and ergonomics

Raised by a review against a live broker during 1.1.1. None of these are bugs; all were
deferred out of the 1.1.1 patch to keep it to fixes.

- **Drop the passive-declare existence checks.** `queue_declare` and `exchange_declare`
  are already idempotent in AMQP, so `_queue_exists` / `_exchange_exists` buy nothing,
  cost a round trip per queue, and were the sole trigger for the confirms bug above.
  Removing them deletes the channel-rebuild path entirely rather than defending it.
- **`delete_queue()` makes three round trips** — existence check, `get_queue_info`, then
  the delete. One suffices.
- **Asymmetric serialization.** `@queue_consumer` JSON-decodes for the callback, but
  `publish_message` requires `str`/`bytes`, so callers must `json.dumps` by hand. Accept
  a dict and encode it.
- **`connection_timeout` maps to pika's `blocked_connection_timeout`**, which is not a
  connect timeout. The name misleads; rename it or document what it does.
- **Batch publisher confirms.** Confirms cost roughly 4x publish throughput when used one
  message at a time (measured: ~2,970 msg/s with confirms versus ~13,200 without, on
  localhost). Batched confirms would recover most of that while keeping the guarantee.

## 1.3.0 — optional async support

- Add `AsyncRabbitMQManager` in a separate module, built on `aio-pika` and published
  behind an `async` extra (`pip install rabbitmq-easy[async]`), so the synchronous path
  keeps its two pure-Python dependencies and its current Python floor.
- Scope the async surface to connect, declare, publish, consume and close. Administrative
  methods (`purge_queue`, `delete_exchange`, `health_check`, `reset_manager`) stay
  sync-only rather than being mirrored.

## Known limitation — shared logger

`RabbitMQManager` logs to a single process-wide logger (`rabbitmq_easy.RabbitMQManager`)
and only attaches handlers when that logger has none. The first manager constructed in a
process therefore decides the logging configuration for every manager created afterwards:
a second instance passing `enable_console_logging=False` still logs to the console if the
first one enabled it.

Options are a per-instance logger, or dropping the handler management entirely and leaving
all configuration to the host application. The latter is the conventional answer for a
library but removes a convenience the README has always documented.

## 2.0.0 — under consideration, not scheduled

- Separate construction from connection. `__init__` currently performs network I/O, so a
  manager cannot be built without a live broker, which makes downstream testing difficult.
  Any fix changes the documented entry point and breaks existing callers.
