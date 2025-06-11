# RabbitMQ Easy

[![PyPI version](https://badge.fury.io/py/rabbitmq-easy.svg)](https://badge.fury.io/py/rabbitmq-easy)
[![Python Support](https://img.shields.io/pypi/pyversions/rabbitmq-easy.svg)](https://pypi.org/project/rabbitmq-easy/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A simple, robust RabbitMQ manager for Python applications with built-in connection management, retry logic, and dead letter queue support.

## 🚀 Features

- **🔄 Automatic Connection Management**: Built-in retry logic and connection recovery
- **🛡️ Dead Letter Queue Support**: Automatic setup of dead letter exchanges and queues
- **📊 Comprehensive Logging**: Both file and console logging with emoji indicators
- **🔧 Environment Variable Support**: Easy configuration through environment variables
- **🧪 Idempotent Operations**: Safe to run multiple times without errors
- **📦 Context Manager Support**: Proper resource cleanup
- **⚡ Production Ready**: Used in production environments
- **🗑️ Resource Management**: Built-in cleanup and deletion functions

## 📦 Installation

```bash
pip install rabbitmq-easy
```

## 🏃‍♂️ Quick Start

### Basic Usage

```python
from rabbitmq_easy import RabbitMQManager

# Simple setup
manager = RabbitMQManager(
    host='localhost',
    port=5672,
    username='guest',
    password='guest',
    queues=['orders', 'payments'],
    routing_keys=['orders.*', 'payments.*'],
    exchange='my_exchange'
)

# Publish a message
manager.publish_message('my_exchange', 'orders.new', '{"order_id": 123}')

# Use as context manager
with RabbitMQManager() as manager:  # Uses environment variables
    manager.publish_message('my_exchange', 'orders.new', '{"order_id": 123}')
```

### Environment Variables Setup

Create a `.env` file:

```bash
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_EXCHANGE=my_exchange
RABBITMQ_QUEUES=orders,payments,notifications
RABBITMQ_ROUTING_KEYS=orders.*,payments.*,notifications.*
```

Then use:

```python
from rabbitmq_easy import create_rabbitmq_manager

# Auto-loads from environment variables
manager = create_rabbitmq_manager()
```

## 🔧 Advanced Usage

### Custom Configuration

```python
from rabbitmq_easy import RabbitMQManager

# Advanced configuration
manager = RabbitMQManager(
    host='rabbitmq.example.com',
    port=5672,
    username='user',
    password='pass',
    queues=['high_priority', 'low_priority'],
    routing_keys=['urgent.*', 'normal.*'],
    exchange='task_exchange',
    dead_letter_exchange='failed_tasks',
    dead_letter_routing_key='failed',
    max_retries=3,
    retry_delay=5,
    enable_console_logging=True,
    log_level='INFO'
)

# Setup additional queues dynamically
manager.setup_queue(
    queue_name='special_queue',
    exchange='task_exchange',
    routing_key='special.*'
)

# Get queue information
info = manager.get_queue_info('high_priority')
print(f"Messages in queue: {info['message_count']}")

# Health check
health = manager.health_check()
print(f"Status: {health['status']}")
```

### Consumer Example

```python
import json
from rabbitmq_easy import RabbitMQManager

def process_message(ch, method, properties, body):
    """Process incoming message"""
    try:
        data = json.loads(body)
        print(f"Processing: {data}")
        
        # Your business logic here
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error processing message: {e}")
        # Reject message (will go to dead letter queue if configured)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Setup consumer
with RabbitMQManager() as manager:
    manager.start_consuming('orders', process_message)
```

### Dead Letter Queue Handling

```python
def handle_failed_messages(ch, method, properties, body):
    """Handle messages that failed processing"""
    try:
        # Get failure information
        headers = properties.headers or {}
        death_info = headers.get('x-death', [])
        
        if death_info:
            failure_reason = death_info[0].get('reason')
            failure_count = death_info[0].get('count')
            print(f"Message failed {failure_count} times. Reason: {failure_reason}")
        
        # Reprocess or log the failure
        data = json.loads(body)
        print(f"Dead letter message: {data}")
        
        # Acknowledge to remove from DLQ
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"Error handling dead letter: {e}")
        # Acknowledge to prevent infinite loops
        ch.basic_ack(delivery_tag=method.delivery_tag)

# Monitor failed messages
manager.start_consuming('failed_messages', handle_failed_messages)
```

## ⚙️ Configuration Options

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `host` | `RABBITMQ_HOST` | `localhost` | RabbitMQ server host |
| `port` | `RABBITMQ_PORT` | `5672` | RabbitMQ server port |
| `username` | `RABBITMQ_USERNAME` | `guest` | Username for authentication |
| `password` | `RABBITMQ_PASSWORD` | `guest` | Password for authentication |
| `queues` | `RABBITMQ_QUEUES` | `[]` | Comma-separated list of queues |
| `routing_keys` | `RABBITMQ_ROUTING_KEYS` | `[]` | Comma-separated list of routing keys |
| `exchange` | `RABBITMQ_EXCHANGE` | `''` | Exchange name |
| `dead_letter_exchange` | `RABBITMQ_DEAD_LETTER_EXCHANGE` | `{exchange}_dlx` | Dead letter exchange name |
| `dead_letter_routing_key` | `RABBITMQ_DEAD_LETTER_ROUTING_KEY` | `dead_letter` | Dead letter routing key |
| `max_retries` | `RABBITMQ_MAX_RETRIES` | `5` | Maximum connection retry attempts |
| `retry_delay` | `RABBITMQ_RETRY_DELAY` | `5` | Delay between retry attempts |
| `heartbeat` | `RABBITMQ_HEARTBEAT` | `60` | Connection heartbeat interval |
| `enable_console_logging` | `RABBITMQ_CONSOLE_LOGGING` | `True` | Enable console logging |
| `log_level` | `RABBITMQ_LOG_LEVEL` | `INFO` | Logging level |

## 🛠️ Resource Management

### Queue and Exchange Operations

```python
# Delete specific resources
manager.delete_queue("old_queue")
manager.delete_exchange("old_exchange")

# Delete only if empty/unused
manager.delete_queue("queue_name", if_empty=True)
manager.delete_exchange("exchange_name", if_unused=True)

# Purge messages without deleting queue
message_count = manager.purge_queue("queue_name")
print(f"Purged {message_count} messages")

# Clean up dead letter infrastructure
manager.cleanup_dead_letter_setup()

# Delete all resources created by this manager
results = manager.delete_all_setup_resources(confirm=True)

# Complete reset
manager.reset_manager(confirm=True)
```

## ❌ Error Handling

The package includes custom exceptions for better error handling:

```python
from rabbitmq_easy import RabbitMQConnectionError, RabbitMQConfigurationError

try:
    manager = RabbitMQManager(
        host='invalid-host',
        queues=['queue1', 'queue2'],
        routing_keys=['key1']  # Mismatch with queues count
    )
except RabbitMQConfigurationError as e:
    print(f"Configuration error: {e}")
except RabbitMQConnectionError as e:
    print(f"Connection error: {e}")
```

## 🏗️ What Gets Created Automatically

When you initialize with:

```python
manager = RabbitMQManager(
    exchange='orders',
    queues=['new_orders', 'pending_orders'],
    routing_keys=['orders.new', 'orders.pending']
)
```

RabbitMQ Easy automatically creates:

1. **`orders`** exchange (main exchange)
2. **`orders_dlx`** exchange (dead letter exchange)
3. **`new_orders`** queue → bound to `orders` exchange with `orders.new` routing key
4. **`pending_orders`** queue → bound to `orders` exchange with `orders.pending` routing key
5. **`failed_messages`** queue → bound to `orders_dlx` exchange for error handling

All queues are configured with dead letter routing to capture failed messages automatically.

## 📋 Best Practices

1. **Use Environment Variables**: Store sensitive information like passwords in environment variables
2. **Context Managers**: Use `with` statements for automatic cleanup
3. **Dead Letter Queues**: Always configure dead letter queues for production
4. **Health Checks**: Implement health checks in your applications
5. **Logging**: Monitor logs for connection issues and errors
6. **Error Handling**: Always handle exceptions in your consumers
7. **Resource Cleanup**: Use the provided cleanup functions during development

## ⚠️ Important Notes

### Queue and Routing Key Validation

The number of queues must match the number of routing keys:

```python
# ✅ Correct
manager = RabbitMQManager(
    queues=['queue1', 'queue2'],
    routing_keys=['key1', 'key2']  # Same count
)

# ❌ Will raise RabbitMQConfigurationError
manager = RabbitMQManager(
    queues=['queue1', 'queue2'],
    routing_keys=['key1']  # Mismatch!
)
```

### Dead Letter Configuration

Don't configure dead letter queues to point to themselves:

```python
# ❌ Don't do this - creates infinite loop
manager.setup_queue(
    'my_dlq',
    dead_letter_exchange='same_exchange'
)

# ✅ Do this instead
manager.setup_queue('my_dlq', dead_letter_exchange=None)
```

## 🔍 Monitoring and Health Checks

```python
# Check connection health
health = manager.health_check()
if health['status'] == 'healthy':
    print("✅ RabbitMQ connection is healthy")
else:
    print(f"❌ RabbitMQ connection issues: {health['error']}")

# Get detailed queue information
info = manager.get_queue_info('orders')
print(f"Queue: {info['queue']}")
print(f"Messages: {info['message_count']}")
print(f"Consumers: {info['consumer_count']}")
```

## 🚀 Production Deployment

### Docker Compose Example

```yaml
version: '3.8'
services:
  app:
    build: .
    environment:
     - ~/etc/<org_name>/.env
    depends_on:
      - rabbitmq
  
  rabbitmq:
    image: rabbitmq:3-management
    environment:
     - ~/etc/<org_name>/.env

```

### Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: rabbitmq-config
data:
  RABBITMQ_HOST: "rabbitmq-service"
  RABBITMQ_PORT: "5672"
  RABBITMQ_EXCHANGE: "production"
  RABBITMQ_QUEUES: "orders,payments,notifications"
  RABBITMQ_ROUTING_KEYS: "orders.*,payments.*,notifications.*"
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for your changes
5. Run the test suite (`pytest`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/Mount-Isaac/rabbitmq-easy/blob/main/LICENSE) file for details.

## 🐛 Support

- Create an issue on [GitHub](https://github.com/mount-isaac/rabbitmq-easy/issues)
- Check the [documentation](https://github.com/mount-isaac/rabbitmq-easy#readme)

## 📚 Changelog

See [CHANGELOG.md](https://github.com/Mount-Isaac/rabbitmq-easy/blob/main/CHANGELOG.md) for a detailed list of changes and version history.

## 🙏 Acknowledgments

- Built on top of the excellent [pika](https://github.com/pika/pika) library
- Inspired by the need for simpler RabbitMQ management in Python applications
- Thanks to the RabbitMQ team for creating an amazing message broker

---

**Made with ❤️ for the Python community**