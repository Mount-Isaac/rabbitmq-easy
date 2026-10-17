# Changelog

All notable changes that have shipped, newest first.
Planned work is tracked separately in [ROADMAP.md](ROADMAP.md).

## [1.1.0] - 2026-10-17

### Security
- **Removed the RabbitMQ password from the connection log line.** `RabbitMQManager.__init__`
  logged `Password:{self.password}` at `INFO`, and the manager installs a `FileHandler`
  at `DEBUG` level, so every downstream application wrote its broker password in plaintext
  to `rabbitmq_manager.log` in its working directory. The line now logs host, port and
  username only. Users upgrading should delete any existing `rabbitmq_manager.log` and
  rotate credentials that appeared in it.

### Fixed
- **Dead letter exchange precedence.** `a or b if c else d` parses as `(a or b) if c else d`,
  so an explicitly passed `dead_letter_exchange` was silently discarded whenever `exchange`
  was empty, and replaced with `default_dead_letter_dlx`.
- **`start_consuming` now re-raises `KeyboardInterrupt`** after stopping the consumer.
  It was previously caught and swallowed, so Ctrl-C never reached the caller and execution
  fell through to the next statement.
- **`create_rabbitmq_manager` no longer calls `sys.exit()`** on interrupt, and no longer
  catches all exceptions to print them and return `None` despite its `-> RabbitMQManager`
  annotation. Connection and configuration errors now propagate as the documented
  exception types.
- **`queue_consumer` logs the actual exception.** It interpolated `{str()}`, which takes no
  argument and always yields an empty string, so every consumer error reported a blank reason.
- **`queue_consumer` works on Python 3.12+.** It used `asyncio.get_event_loop()`, which
  raises `RuntimeError` when no loop is running; async callbacks now use `asyncio.run()`.
- **`queue_consumer` sets `wrapper._queue`**, previously misspelled `_quueu`.
- **`validate_queue_routing_match` returns a bool.** It used a bare `assert` (removed under
  `python -O`) and returned `None` on success, making the success path falsy.

### Changed
- **The library no longer writes `rabbitmq_manager.log`.** It previously created that file
  in the host application's working directory on every run. File logging is now opt-in via
  the new `log_file` parameter, and the package logger carries a `NullHandler` so the host
  application controls logging. Console logging is unchanged and still on by default.
- **`load_dotenv()` is no longer called at import time.** Importing the package mutated
  `os.environ` for the entire host application as a side effect. The `.env` file is now
  read when a manager is constructed, and can be disabled with `load_env=False`.
- **Every documented environment variable is now read.** `RABBITMQ_QUEUES`,
  `RABBITMQ_ROUTING_KEYS`, `RABBITMQ_DEAD_LETTER_EXCHANGE`,
  `RABBITMQ_DEAD_LETTER_ROUTING_KEY`, `RABBITMQ_MAX_RETRIES`, `RABBITMQ_RETRY_DELAY`,
  `RABBITMQ_HEARTBEAT`, `RABBITMQ_CONSOLE_LOGGING` and `RABBITMQ_LOG_LEVEL` were all
  documented but never read; only the five connection variables took effect. **If you
  already have any of these set in a `.env` file they will now take effect** — in
  particular, `RABBITMQ_QUEUES` and `RABBITMQ_ROUTING_KEYS` will cause queues to be
  declared on startup where previously they were ignored. Check your environment before
  upgrading. Keyword arguments still take precedence, and an explicit empty list means
  empty rather than falling back to the environment.
- A non-numeric value in an integer variable now raises `RabbitMQConfigurationError`
  naming the variable, rather than being silently ignored.
- Minimum supported Python is now 3.9. 3.7 and 3.8 are end-of-life, and the classifiers
  previously advertised 3.7 alongside 3.12/3.13 support that the async path did not have.
- `__version__` is read from installed package metadata rather than hardcoded, leaving
  git tags as the single source of version truth.
- `requirements.txt` now lists only runtime dependencies. It and `requirements-dev.txt`
  were previously identical `pip freeze` output of the development environment.

### Removed
- `setup.py`. It duplicated `pyproject.toml` with conflicting metadata: a hardcoded version,
  placeholder `yourusername` project URLs, and `install_requires` read from the `pip freeze`
  above, which would have published around 40 development tools as runtime dependencies if
  anything had built with it. Its classifiers, keywords and documentation URL were moved
  into `pyproject.toml`.
- `tests/` is no longer included in the built wheel.

### Added
- **Publisher confirms, enabled by default.** `publish_message()` now waits for the broker
  to acknowledge the message and raises if it is rejected. Previously a publish returned as
  soon as the bytes reached the socket, so a broker crash lost messages that appeared to
  have been sent. Confirms cost a round-trip per publish; disable with
  `publisher_confirms=False` or `RABBITMQ_PUBLISHER_CONFIRMS=false`.
- **`mandatory` argument to `publish_message()`.** When set, a message that cannot be
  routed to any queue raises instead of being silently discarded by the broker. Unroutable
  messages are not retried, since retrying cannot help.
- **Consumer reconnection.** `start_consuming()` now reconnects and resumes when the
  connection drops, giving up after `max_retries` consecutive failures; a consumer that
  stayed connected for at least a minute before dropping gets a fresh budget. Previously a
  dropped connection raised `RabbitMQManagerError` and ended the consumer, despite the
  README advertising connection recovery. Pass `reconnect=False` for the old behaviour.
- CI running the test suite on Python 3.9 through 3.13, plus a build job that verifies
  distribution metadata with `twine check`.
- `log_file` parameter. Pass a path to write a log file; none is written by default.
- Test coverage for environment variable handling, and a `conftest.py` that isolates the
  suite from ambient `RABBITMQ_*` variables and from any local `.env` file. The suite
  previously behaved differently on a developer machine than in CI.
- `load_env` parameter to disable reading a `.env` file.
- README section documenting shutdown, cleanup, consumer interruption and logging.

## [1.0.5] - 2025-08-27

### Changed
- updated readme documentation

## [1.0.4] - 2025-06-11

### Changed
- removed static dead_letter_queue_name (dlq_name) value from failed_messages 
    to a passed parameter in the class constructor with a default value of ```failed_messages```

## [1.0.1] - 2025-06-11

### Changed
- Updated README with improved documentation
- Fixed typos in examples
- Enhanced Docker Compose examples
- Improved formatting and clarity

## [1.0.0] - 2025-06-10

### Added
- Initial release of RabbitMQ Easy
- Automatic connection management with retry logic
- Dead letter queue support
- Environment variable configuration
- Comprehensive logging with emoji indicators
- Context manager support
- Health check functionality
- Queue information retrieval
- Batch queue setup
- Custom exceptions for better error handling
- Complete test suite
- Documentation and examples

### Features
- Idempotent queue and exchange creation
- Automatic reconnection on connection loss
- Configurable retry logic
- Support for both file and console logging
- Production-ready error handling
