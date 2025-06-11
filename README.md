# RabbitMQ Easy

[![PyPI version](https://badge.fury.io/py/rabbitmq-easy.svg)](https://badge.fury.io/py/rabbitmq-easy)
[![Python Support](https://img.shields.io/pypi/pyversions/rabbitmq-easy.svg)](https://pypi.org/project/rabbitmq-easy/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A simple, robust RabbitMQ manager for Python applications with built-in connection management, retry logic, and dead letter queue support.

## 🚀 Features

- **🔄 Automatic Connection Management** with retry logic
- **🛡️ Dead Letter Queue Support** with automatic setup
- **🔧 Environment Variable Support** for easy configuration
- **📦 Context Manager Support** for proper cleanup
- **⚡ Production Ready** with comprehensive error handling

## 📦 Installation

```bash
pip install rabbitmq-easy
```

## 🏃‍♂️ Quick Start

```python
from rabbitmq_easy import RabbitMQManager

# Simple setup
manager = RabbitMQManager(
    host='localhost',
    queues=['orders', 'payments'],
    routing_keys=['orders.*', 'payments.*'],
    exchange='my_exchange'
)

# Publish a message
manager.publish_message('my_exchange', 'orders.new', '{"order_id": 123}')

# Use as context manager
with RabbitMQManager() as manager:
    manager.publish_message('my_exchange', 'orders.new', '{"order_id": 123}')
```

## 🔧 Environment Variables

```bash
# .env file
RABBITMQ_HOST=localhost
RABBITMQ_USERNAME=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_EXCHANGE=my_exchange
RABBITMQ_QUEUES=orders,payments
RABBITMQ_ROUTING_KEYS=orders.*,payments.*
```

```python
from rabbitmq_easy import create_rabbitmq_manager

# Auto-loads from environment
manager = create_rabbitmq_manager()
```

## 📖 Usage Examples

### Consumer
```python
import json

def process_message(ch, method, properties, body):
    try:
        data = json.loads(body)
        print(f"Processing: {data}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Error: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

manager.start_consuming('orders', process_message)
```

### Dead Letter Handling
```python
def handle_failed_messages(ch, method, properties, body):
    headers = properties.headers or {}
    death_info = headers.get('x-death', [])
    
    if death_info:
        print(f"Message failed {death_info[0].get('count')} times")
    
    # Reprocess or log
    ch.basic_ack(delivery_tag=method.delivery_tag)

manager.start_consuming('failed_messages', handle_failed_messages)
```

## ⚙️ Configuration

| Parameter | Environment Variable | Default |
|-----------|---------------------|---------|
| `host` | `RABBITMQ_HOST` | `localhost` |
| `username` | `RABBITMQ_USERNAME` | `guest` |
| `password` | `RABBITMQ_PASSWORD` | `guest` |
| `queues` | `RABBITMQ_QUEUES` | `[]` |
| `routing_keys` | `RABBITMQ_ROUTING_KEYS` | `[]` |
| `exchange` | `RABBITMQ_EXCHANGE` | `''` |

## 🏗️ What Gets Created

When you initialize:
```python
manager = RabbitMQManager(
    exchange='orders',
    queues=['new_orders', 'pending_orders'],
    routing_keys=['orders.new', 'orders.pending']
)
```

**Automatically creates:**
1. `orders` exchange
2. `orders_dlx` exchange (dead letter)
3. `new_orders` queue → bound to `orders`
4. `pending_orders` queue → bound to `orders`
5. `failed_messages` queue → bound to `orders_dlx`

## 🛠️ Resource Management

```python
# Queue operations
manager.delete_queue("old_queue")
manager.purge_queue("queue_name")
info = manager.get_queue_info('orders')

# Health check
health = manager.health_check()

# Cleanup
manager.delete_all_setup_resources(confirm=True)
```

## ❌ Error Handling

```python
from rabbitmq_easy import RabbitMQConnectionError, RabbitMQConfigurationError

try:
    manager = RabbitMQManager(
        queues=['queue1', 'queue2'],
        routing_keys=['key1']  # Mismatch - will raise error
    )
except RabbitMQConfigurationError as e:
    print(f"Configuration error: {e}")
```

## 🚀 Production Example

```yaml
# docker-compose.yml
version: '3.8'
services:
  app:
    build: .
    env_file: .env
    depends_on: [rabbitmq]
  
  rabbitmq:
    image: rabbitmq:3-management
    env_file: .env
```

## 📋 Best Practices

- Use environment variables for credentials
- Always configure dead letter queues for production
- Use context managers for automatic cleanup
- Monitor queue lengths and consumer lag

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for changes
4. Submit a Pull Request

## 📝 License & Support

- **License**: MIT - see [LICENSE](https://github.com/Mount-Isaac/rabbitmq-easy/blob/main/LICENSE)
- **Issues**: [GitHub Issues](https://github.com/Mount-Isaac/rabbitmq-easy/issues)  
- **Docs**: [GitHub README](https://github.com/Mount-Isaac/rabbitmq-easy#readme)

---

**Made with ❤️ for the Python community**