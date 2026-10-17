"""Shared test fixtures."""

import logging
import os
import pytest


@pytest.fixture(autouse=True)
def isolate_rabbitmq_env(monkeypatch):
    """Keep tests independent of ambient configuration.

    Without this, the suite reads whatever ``RABBITMQ_*`` variables happen to be
    set, or a developer's local ``.env`` file, and behaves differently on a
    workstation than it does in CI, where no ``.env`` is present.
    """
    for key in list(os.environ):
        if key.startswith('RABBITMQ_'):
            monkeypatch.delenv(key, raising=False)

    # stop the manager repopulating them from a .env file on disk
    monkeypatch.setattr(
        'rabbitmq_easy.manager.load_dotenv', lambda *args, **kwargs: None
    )


@pytest.fixture(autouse=True)
def reset_manager_logger():
    """Clear handlers between tests.

    ``RabbitMQManager`` logs to a process-wide logger and only configures handlers
    when that logger has none, so the first manager created in a process decides
    the logging configuration for every later one. Without this reset, a test's
    logging assertions would depend on which tests ran before it.
    """
    logger = logging.getLogger('rabbitmq_easy.RabbitMQManager')
    logger.handlers.clear()
    yield
    logger.handlers.clear()
