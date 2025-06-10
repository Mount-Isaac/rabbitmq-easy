"""
Custom exceptions for RabbitMQ Easy package.
"""

class RabbitMQManagerError(Exception):
    """Base exception for RabbitMQ Manager errors."""
    pass

class RabbitMQConnectionError(RabbitMQManagerError):
    """Raised when connection to RabbitMQ fails."""
    pass

class RabbitMQConfigurationError(RabbitMQManagerError):
    """Raised when configuration is invalid."""
    pass

