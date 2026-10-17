# Roadmap

Planned work for `rabbitmq-easy`, in the order it should be done.
Each heading is a release boundary: finish and ship one before starting the next.

Shipped changes are recorded in [CHANGELOG.md](CHANGELOG.md).

## 1.2.0 — optional async support (next)

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
