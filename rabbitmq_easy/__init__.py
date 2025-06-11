from .exceptions import (
    RabbitMQManagerError, 
    RabbitMQConfigurationError, 
    RabbitMQConnectionError
)

from .manager import RabbitMQManager, create_rabbitmq_manager

__version__ = "1.0.3"
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