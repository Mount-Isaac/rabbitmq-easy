# rabbitmq_easy/manager.py
import pika
import logging
import os
import time
import sys
from typing import Optional, List, Dict, Callable, Any
from dotenv import load_dotenv
from .exceptions import (
    RabbitMQConnectionError,
    RabbitMQConfigurationError, 
    RabbitMQManagerError
)

# Load environment variables
load_dotenv()


class RabbitMQManager:
    """
    A robust RabbitMQ manager for easy queue and exchange management.
    
    Features:
    - Automatic connection management with retry logic
    - Idempotent queue and exchange creation
    - Dead letter queue support
    - Comprehensive logging
    - Context manager support
    - Environment variable configuration
    """
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        username: str = None,
        password: str = None,
        queues: List[str] = None,
        routing_keys: List[str] = None,
        exchange: str = '',
        exchange_type: str = 'topic',
        dead_letter_exchange: str = None,
        dead_letter_routing_key: str = 'dead_letter',
        heartbeat: int = 60,
        connection_timeout: int = 300,
        max_retries: int = 5,
        retry_delay: int = 5,
        prefetch_count: int = 1,
        enable_console_logging: bool = True,
        log_level: str = 'INFO'
    ):
        # Setup logging first
        self._setup_logging(enable_console_logging, log_level)
        
        # Load configuration from environment variables if not provided
        self.host = host or os.getenv('RABBITMQ_HOST', 'localhost')
        self.port = int(port or os.getenv('RABBITMQ_PORT', 5672))
        self.username = username or os.getenv('RABBITMQ_USERNAME', 'guest')
        self.password = password or os.getenv('RABBITMQ_PASSWORD', 'guest')
        
        # Queue and routing configuration
        self.queues = queues or []
        self.routing_keys = routing_keys or []
        self.exchange = exchange or os.getenv('RABBITMQ_EXCHANGE', '')
        self.exchange_type = exchange_type
        
        # Dead letter configuration with smart defaults
        self.dead_letter_exchange = dead_letter_exchange or f"{self.exchange}_dlx" if self.exchange else 'default_dead_letter_dlx'
        self.dead_letter_routing_key = dead_letter_routing_key
        
        # Connection parameters
        self.heartbeat = int(heartbeat)
        self.connection_timeout = int(connection_timeout)
        self.max_retries = int(max_retries)
        self.retry_delay = int(retry_delay)
        self.prefetch_count = int(prefetch_count)
        
        # Validate configuration
        self._validate_configuration()
        
        # Connection objects
        self.connection = None
        self.channel = None
        
        # Track what's been set up to avoid duplication
        self._setup_exchanges = set()
        self._setup_queues = set()
        
        self.logger.info(f"Initializing RabbitMQ Manager for {self.host}:{self.port}")
        self.logger.info(f"Exchange: {self.exchange}, Dead Letter Exchange: {self.dead_letter_exchange}")
        
        # Initialize connection
        self._initialize_connection()
        
        # Setup queues if provided
        if self.queues:
            self._setup_initial_queues()

    def _setup_logging(self, enable_console: bool, log_level: str) -> None:
        """Setup logging configuration."""
        self.logger = logging.getLogger(f"rabbitmq_easy.{self.__class__.__name__}")
        self.logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        
        # Avoid duplicate handlers
        if not self.logger.handlers:
            # File handler
            file_handler = logging.FileHandler('rabbitmq_manager.log')
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                'Date-Time: %(asctime)s - %(name)s - %(levelname)s - Line: %(lineno)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)
            
            # Console handler
            if enable_console:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
                console_formatter = logging.Formatter(
                    '%(asctime)s - RabbitMQ - %(levelname)s - %(message)s'
                )
                console_handler.setFormatter(console_formatter)
                self.logger.addHandler(console_handler)

    def _validate_configuration(self) -> None:
        """Validate the configuration parameters."""
        # Validate queues and routing keys match
        if self.queues and self.routing_keys:
            if len(self.queues) != len(self.routing_keys):
                error_msg = (
                    f"Queues and routing keys count mismatch: "
                    f"queues={len(self.queues)}, routing_keys={len(self.routing_keys)}. "
                    f"Each queue must have a corresponding routing key."
                )
                self.logger.error(error_msg)
                raise RabbitMQConfigurationError(error_msg)
        
        # Validate connection parameters
        if not all([self.host, self.username, self.password]):
            raise RabbitMQConfigurationError("Host, username, and password are required")
        
        if not 1 <= self.port <= 65535:
            raise RabbitMQConfigurationError(f"Invalid port number: {self.port}")
        
        self.logger.info("Configuration validation passed")

    def _get_connection_parameters(self) -> pika.ConnectionParameters:
        """Create connection parameters with retry policy."""
        credentials = pika.PlainCredentials(
            username=self.username,
            password=self.password
        )
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat=self.heartbeat,
            blocked_connection_timeout=self.connection_timeout,
            connection_attempts=self.max_retries,
            retry_delay=self.retry_delay
        )

    def _initialize_connection(self) -> None:
        """Initialize connection and channel with comprehensive error handling."""
        retries = 0
        while retries < self.max_retries:
            try:
                self.logger.info(f"Attempting to connect to RabbitMQ at {self.host}:{self.port} (attempt {retries + 1}/{self.max_retries})")
                params = self._get_connection_parameters()
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=self.prefetch_count)
                self.logger.info("✅ Successfully established RabbitMQ connection")
                return
            except pika.exceptions.AMQPConnectionError as e:
                retries += 1
                self.logger.error(f"❌ AMQP Connection failed (attempt {retries}/{self.max_retries}): {str(e)}")
            except Exception as e:
                retries += 1
                self.logger.error(f"❌ Connection failed (attempt {retries}/{self.max_retries}): {str(e)}")
            
            if retries < self.max_retries:
                self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
        
        error_msg = f"Failed to establish RabbitMQ connection after {self.max_retries} attempts"
        self.logger.error(error_msg)
        raise RabbitMQConnectionError(error_msg)

    def ensure_connection(self) -> None:
        """Ensure connection is active, reconnect if necessary."""
        try:
            if self.connection is None or self.connection.is_closed:
                self.logger.warning("Connection lost, reconnecting...")
                self._initialize_connection()
            elif self.channel is None or self.channel.is_closed:
                self.logger.warning("Channel lost, recreating...")
                self.channel = self.connection.channel()
                self.channel.basic_qos(prefetch_count=self.prefetch_count)
        except Exception as e:
            self.logger.error(f"Connection error: {str(e)}")
            self._initialize_connection()

    def _exchange_exists(self, exchange_name: str) -> bool:
        """Check if exchange exists."""
        if not exchange_name:
            return True  # Default exchange always exists
            
        try:
            self.ensure_connection()
            self.channel.exchange_declare(exchange=exchange_name, passive=True)
            return True
        except pika.exceptions.ChannelClosedByBroker:
            # Exchange doesn't exist, need to recreate channel
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            return False
        except Exception:
            return False

    def _queue_exists(self, queue_name: str) -> bool:
        """Check if queue exists."""
        try:
            self.ensure_connection()
            self.channel.queue_declare(queue=queue_name, passive=True)
            return True
        except pika.exceptions.ChannelClosedByBroker:
            # Queue doesn't exist, need to recreate channel
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            return False
        except Exception:
            return False

    def setup_exchange(self, exchange_name: str, exchange_type: str = None, durable: bool = True) -> None:
        """Setup exchange if it doesn't exist."""
        self.logger.info(f"Setting up exchnage: {exchange_name}")
        if not exchange_name or exchange_name in self._setup_exchanges:
            return
            
        exchange_type = exchange_type or self.exchange_type
        
        try:
            self.ensure_connection()
            
            if not self._exchange_exists(exchange_name):
                self.channel.exchange_declare(
                    exchange=exchange_name,
                    exchange_type=exchange_type,
                    durable=durable
                )
                self.logger.info(f"🔄 Created exchange: {exchange_name} (type: {exchange_type})")
            else:
                self.logger.info(f"✅ Exchange already exists: {exchange_name}")
            
            self._setup_exchanges.add(exchange_name)
            
        except Exception as e:
            error_msg = f"Failed to setup exchange {exchange_name}: {str(e)}"
            self.logger.error(error_msg)
            raise RabbitMQManagerError(error_msg)

    def setup_queue(self, 
                   queue_name: str, 
                   is_dlq: bool = False,
                   exchange: str = None, 
                   routing_key: str = None,
                   durable: bool = True,
                   dead_letter_exchange: str = None,
                   dead_letter_routing_key: str = None) -> None:
        """Setup queue with exchange binding."""
        
        # Use instance defaults if not provided
        exchange = exchange or self.exchange
        dead_letter_exchange = dead_letter_exchange or self.dead_letter_exchange
        dead_letter_routing_key = dead_letter_routing_key or self.dead_letter_routing_key
        
        queue_key = f"{queue_name}:{exchange}:{routing_key}"
        if queue_key in self._setup_queues:
            self.logger.debug(f"Queue setup already completed: {queue_name}")
            return
            
        try:
            self.ensure_connection()
            
            # Setup main exchange first if provided
            if exchange:
                self.setup_exchange(exchange, self.exchange_type)
            
            # Setup dead letter exchange
            if dead_letter_exchange:
                # and not dead_letter_exchange.endswith('_dlx'):
                # Ensure we don't create DLX for DLX itself
                self.setup_exchange(dead_letter_exchange, self.exchange_type)
            
            # Prepare queue arguments for dead letter routing
            arguments = {}
            if not is_dlq:
                if dead_letter_exchange and dead_letter_routing_key and not queue_name.endswith('_dlq'):
                    arguments = {
                        'x-dead-letter-exchange': dead_letter_exchange,
                        'x-dead-letter-routing-key': dead_letter_routing_key
                    }
            
            # Create queue if it doesn't exist
            if not self._queue_exists(queue_name):
                self.channel.queue_declare(
                    queue=queue_name, 
                    durable=durable, 
                    arguments=arguments if arguments else None
                )
                self.logger.info(f"🔄 Created queue: {queue_name}")
                if arguments:
                    self.logger.info(f"📧 Dead letter routing: {dead_letter_exchange} -> {dead_letter_routing_key}")
            else:
                self.logger.info(f"✅ Queue already exists: {queue_name}")
            
            # Bind queue to exchange if both are provided
            if exchange and routing_key:
                self.channel.queue_bind(
                    exchange=exchange,
                    queue=queue_name,
                    routing_key=routing_key
                )
                self.logger.info(f"🔗 Bound queue '{queue_name}' to exchange '{exchange}' with routing key '{routing_key}'")
            
            self._setup_queues.add(queue_key)
            
        except Exception as e:
            error_msg = f"Failed to setup queue {queue_name}: {str(e)}"
            self.logger.error(error_msg)
            raise RabbitMQManagerError(error_msg)

    def _setup_initial_queues(self) -> None:
        """Setup queues from initial configuration."""
        self.logger.info(f"Setting up {len(self.queues)} initial queues...")
        
        for i, queue_name in enumerate(self.queues):
            routing_key = self.routing_keys[i] if i < len(self.routing_keys) else None
            self.setup_queue(
                queue_name=queue_name,
                exchange=self.exchange,
                routing_key=routing_key,
                dead_letter_exchange=self.dead_letter_exchange,
                dead_letter_routing_key=self.dead_letter_routing_key
            )
        
        self.logger.info("✅ Initial queue setup completed")

        if self.dead_letter_exchange:
            dlq_name = "failed_messages"  # Simple, clear name
            self.setup_queue(
                queue_name=dlq_name,
                exchange=self.dead_letter_exchange,
                routing_key=self.dead_letter_routing_key,
                dead_letter_exchange=None,  # No DLX for DLQ itself
                dead_letter_routing_key=None,
                is_dlq=True,
            )
            self.logger.info(f"🗂️ Auto-created dead letter queue: {dlq_name}")

    def setup_queues_batch(self, queues_config: List[Dict]) -> None:
        """Setup multiple queues from configuration list."""
        self.logger.info(f"Setting up {len(queues_config)} queues from batch configuration...")
        
        for config in queues_config:
            self.setup_queue(
                queue_name=config['queue'],
                exchange=config.get('exchange', self.exchange),
                routing_key=config.get('routing_key', ''),
                durable=config.get('durable', True),
                dead_letter_exchange=config.get('dead_letter_exchange', self.dead_letter_exchange),
                dead_letter_routing_key=config.get('dead_letter_routing_key', self.dead_letter_routing_key)
            )

    def publish_message(self, 
                       exchange: str, 
                       routing_key: str, 
                       message: str,
                       persistent: bool = True) -> None:
        """Publish message with connection check and retry."""
        retries = 0
        while retries < self.max_retries:
            try:
                self.ensure_connection()
                properties = pika.BasicProperties(
                    delivery_mode=2 if persistent else 1
                )
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=message,
                    properties=properties
                )
                self.logger.debug(f"📤 Published message to exchange: {exchange}, routing_key: {routing_key}")
                return
            except Exception as e:
                retries += 1
                self.logger.error(f"❌ Failed to publish message (attempt {retries}/{self.max_retries}): {str(e)}")
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                    self.connection = None  # Force reconnection
        
        error_msg = f"Failed to publish message after {self.max_retries} attempts"
        self.logger.error(error_msg)
        raise RabbitMQManagerError(error_msg)

    def start_consuming(self, 
                       queue_name: str, 
                       callback: Callable, 
                       auto_ack: bool = False) -> None:
        """Start consuming messages from a queue."""
        try:
            self.ensure_connection()
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=auto_ack
            )
            self.logger.info(f"📥 Started consuming from queue: {queue_name}")
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.info("Consumption interrupted by user")
            self.stop_consuming()
        except Exception as e:
            error_msg = f"Error in consumer: {str(e)}"
            self.logger.error(error_msg)
            raise RabbitMQManagerError(error_msg)

    def stop_consuming(self) -> None:
        """Stop consuming messages."""
        if self.channel and not self.channel.is_closed:
            self.channel.stop_consuming()
            self.logger.info("⏹️ Stopped consuming messages")

    def get_queue_info(self, queue_name: str) -> Dict:
        """Get information about a queue."""
        try:
            self.ensure_connection()
            method = self.channel.queue_declare(queue=queue_name, passive=True)
            return {
                'queue': queue_name,
                'message_count': method.method.message_count,
                'consumer_count': method.method.consumer_count
            }
        except Exception as e:
            error_msg = f"Failed to get queue info for {queue_name}: {str(e)}"
            self.logger.error(error_msg)
            raise RabbitMQManagerError(error_msg)


    def delete_queue(self, queue_name: str, if_unused: bool = False, if_empty: bool = False) -> bool:
        """
        Delete a queue.
        
        Args:
            queue_name: Name of the queue to delete
            if_unused: Only delete if queue has no consumers
            if_empty: Only delete if queue has no messages
            
        Returns:
            bool: True if deleted successfully, False otherwise
        """
        try:
            self.ensure_connection()
            
            # Check if queue exists first
            if not self._queue_exists(queue_name):
                self.logger.warning(f"⚠️ Queue '{queue_name}' does not exist")
                return False
            
            # Get queue info before deletion
            queue_info = self.get_queue_info(queue_name)
            message_count = queue_info.get('message_count', 0)
            consumer_count = queue_info.get('consumer_count', 0)
            
            # Check conditions
            if if_unused and consumer_count > 0:
                self.logger.warning(f"⚠️ Cannot delete queue '{queue_name}' - has {consumer_count} consumers")
                return False
                
            if if_empty and message_count > 0:
                self.logger.warning(f"⚠️ Cannot delete queue '{queue_name}' - has {message_count} messages")
                return False
            
            # Delete the queue
            self.channel.queue_delete(queue=queue_name, if_unused=if_unused, if_empty=if_empty)
            
            # Remove from tracking
            keys_to_remove = [key for key in self._setup_queues if key.startswith(f"{queue_name}:")]
            for key in keys_to_remove:
                self._setup_queues.discard(key)
            
            self.logger.info(f"🗑️ Deleted queue: {queue_name}")
            if message_count > 0:
                self.logger.warning(f"⚠️ Deleted queue had {message_count} messages")
                
            return True
            
        except pika.exceptions.ChannelClosedByBroker as e:
            if "NOT_FOUND" in str(e):
                self.logger.warning(f"⚠️ Queue '{queue_name}' not found")
            else:
                self.logger.error(f"❌ Failed to delete queue '{queue_name}': {str(e)}")
            # Recreate channel after it was closed
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to delete queue '{queue_name}': {str(e)}")
            return False

    def delete_exchange(self, exchange_name: str, if_unused: bool = False) -> bool:
        """
        Delete an exchange.
        
        Args:
            exchange_name: Name of the exchange to delete
            if_unused: Only delete if exchange has no queue bindings
            
        Returns:
            bool: True if deleted successfully, False otherwise
        """
        try:
            self.ensure_connection()
            
            # Cannot delete default exchange
            if not exchange_name:
                self.logger.error("❌ Cannot delete default exchange")
                return False
            
            # Check if exchange exists first
            if not self._exchange_exists(exchange_name):
                self.logger.warning(f"⚠️ Exchange '{exchange_name}' does not exist")
                return False
            
            # Delete the exchange
            self.channel.exchange_delete(exchange=exchange_name, if_unused=if_unused)
            
            # Remove from tracking
            self._setup_exchanges.discard(exchange_name)
            
            self.logger.info(f"🗑️ Deleted exchange: {exchange_name}")
            return True
            
        except pika.exceptions.ChannelClosedByBroker as e:
            if "NOT_FOUND" in str(e):
                self.logger.warning(f"⚠️ Exchange '{exchange_name}' not found")
            elif "PRECONDITION_FAILED" in str(e):
                self.logger.error(f"❌ Cannot delete exchange '{exchange_name}' - still has bindings (use if_unused=True)")
            else:
                self.logger.error(f"❌ Failed to delete exchange '{exchange_name}': {str(e)}")
            # Recreate channel after it was closed
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to delete exchange '{exchange_name}': {str(e)}")
            return False

    def delete_all_setup_resources(self, confirm: bool = False) -> Dict[str, bool]:
        """
        Delete all exchanges and queues that were set up by this manager instance.
        
        Args:
            confirm: Must be True to actually delete (safety measure)
            
        Returns:
            dict: Results of deletion operations
        """
        if not confirm:
            self.logger.error("❌ Must set confirm=True to delete all resources")
            return {}
        
        results = {
            'queues_deleted': [],
            'queues_failed': [],
            'exchanges_deleted': [],
            'exchanges_failed': []
        }
        
        self.logger.warning("🚨 Starting deletion of ALL setup resources...")
        
        # Delete queues first (they depend on exchanges)
        for queue_key in list(self._setup_queues):
            queue_name = queue_key.split(':')[0]
            if self.delete_queue(queue_name):
                results['queues_deleted'].append(queue_name)
            else:
                results['queues_failed'].append(queue_name)
        
        # Delete exchanges
        for exchange_name in list(self._setup_exchanges):
            if self.delete_exchange(exchange_name):
                results['exchanges_deleted'].append(exchange_name)
            else:
                results['exchanges_failed'].append(exchange_name)
        
        # Summary
        total_deleted = len(results['queues_deleted']) + len(results['exchanges_deleted'])
        total_failed = len(results['queues_failed']) + len(results['exchanges_failed'])
        
        self.logger.info(f"🗑️ Deletion complete: {total_deleted} deleted, {total_failed} failed")
        
        return results

    def purge_queue(self, queue_name: str) -> int:
        """
        Remove all messages from a queue without deleting the queue.
        
        Args:
            queue_name: Name of the queue to purge
            
        Returns:
            int: Number of messages purged, -1 if failed
        """
        try:
            self.ensure_connection()
            
            # Check if queue exists
            if not self._queue_exists(queue_name):
                self.logger.warning(f"⚠️ Queue '{queue_name}' does not exist")
                return -1
            
            # Purge the queue
            method = self.channel.queue_purge(queue=queue_name)
            message_count = method.method.message_count
            
            self.logger.info(f"🧹 Purged {message_count} messages from queue: {queue_name}")
            return message_count
            
        except Exception as e:
            self.logger.error(f"❌ Failed to purge queue '{queue_name}': {str(e)}")
            return -1

    def cleanup_dead_letter_setup(self) -> bool:
        """
        Clean up dead letter infrastructure (DLX and DLQ).
        Useful for resetting dead letter configuration.
        
        Returns:
            bool: True if successful
        """
        try:
            dlq_deleted = False
            dlx_deleted = False
            
            # Delete the auto-created dead letter queue
            if self.delete_queue("failed_messages"):
                dlq_deleted = True
                
            # Delete the dead letter exchange if it exists
            if self.dead_letter_exchange and self.delete_exchange(self.dead_letter_exchange):
                dlx_deleted = True
            
            self.logger.info(f"🧹 Dead letter cleanup: DLQ={'✅' if dlq_deleted else '❌'}, DLX={'✅' if dlx_deleted else '❌'}")
            return dlq_deleted or dlx_deleted
            
        except Exception as e:
            self.logger.error(f"❌ Failed to cleanup dead letter setup: {str(e)}")
            return False

    def reset_manager(self, confirm: bool = False) -> bool:
        """
        Reset the manager by deleting all resources and clearing tracking.
        
        Args:
            confirm: Must be True to actually reset
            
        Returns:
            bool: True if successful
        """
        if not confirm:
            self.logger.error("❌ Must set confirm=True to reset manager")
            return False
        
        try:
            # Delete all resources
            results = self.delete_all_setup_resources(confirm=True)
            
            # Clear tracking sets
            self._setup_exchanges.clear()
            self._setup_queues.clear()
            
            self.logger.info("🔄 Manager reset complete - all resources deleted and tracking cleared")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to reset manager: {str(e)}")
            return False

    def health_check(self) -> Dict[str, Any]:
        """Perform a health check on the RabbitMQ connection."""
        try:
            self.ensure_connection()
            # Try to declare a temporary queue to test the connection
            self.channel.queue_declare(queue='health_check_temp', exclusive=True, auto_delete=True)
            return {
                'status': 'healthy',
                'host': self.host,
                'port': self.port,
                'exchanges_setup': len(self._setup_exchanges),
                'queues_setup': len(self._setup_queues)
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'host': self.host,
                'port': self.port
            }

    def close(self) -> None:
        """Safely close the connection."""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            self.channel = None
            self.connection = None
            self.logger.info("🔌 RabbitMQ connection closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing connection: {str(e)}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __repr__(self) -> str:
        return f"RabbitMQManager(host={self.host}, port={self.port}, exchange={self.exchange})"
    


def create_rabbitmq_manager(**kwargs) -> RabbitMQManager:
    """
    Create a RabbitMQ manager with environment variable support.
    
    Usage:
        manager = create_rabbitmq_manager(
            queues=['queue1', 'queue2'],
            routing_keys=['key1', 'key2']
        )
    """
    return RabbitMQManager(**kwargs)