"""
Utility functions for RabbitMQ Easy package.
"""

import os
from typing import List, Dict, Any

def parse_env_list(env_var: str, default: List[str] = None) -> List[str]:
    """Parse comma-separated environment variable into list."""
    value = os.getenv(env_var, '')
    if not value:
        return default or []
    return [item.strip() for item in value.split(',') if item.strip()]

def create_queue_config_from_env() -> Dict[str, Any]:
    """Create queue configuration from environment variables."""
    return {
        'queues': parse_env_list('RABBITMQ_QUEUES'),
        'routing_keys': parse_env_list('RABBITMQ_ROUTING_KEYS'),
        'exchange': os.getenv('RABBITMQ_EXCHANGE', ''),
        'dead_letter_exchange': os.getenv('RABBITMQ_DEAD_LETTER_EXCHANGE'),
        'dead_letter_routing_key': os.getenv('RABBITMQ_DEAD_LETTER_ROUTING_KEY', 'dead_letter')
    }

def validate_queue_routing_match(queues: List[str], routing_keys: List[str]) -> bool:
    """Validate that queues and routing keys have matching lengths."""
    if not queues or not routing_keys:
        return True
    assert len(queues) == len(routing_keys)

