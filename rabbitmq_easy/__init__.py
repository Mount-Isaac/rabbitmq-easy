import logging

# Libraries should not configure logging; the host application decides.
logging.getLogger(__name__).addHandler(logging.NullHandler())

from .exceptions import (
    RabbitMQManagerError, 
    RabbitMQConfigurationError, 
    RabbitMQConnectionError
)

from .manager import RabbitMQManager, create_rabbitmq_manager

try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:  # pragma: no cover - Python < 3.8
    from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("rabbitmq-easy")
except PackageNotFoundError:  # running from a source checkout, not installed
    __version__ = "0.0.0+unknown"

__author__ = "Isaac Kyalo"
__email__ = "isadechair019@gmail.com"
__description__ = "A simple, robust RabbitMQ manager for Python applications"

__all__ = [
    'RabbitMQManager',
    'create_rabbitmq_manager',
    'RabbitMQManagerError',
    'RabbitMQConnectionError',
    'RabbitMQConfigurationError'
]