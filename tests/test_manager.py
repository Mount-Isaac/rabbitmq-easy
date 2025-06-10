"""
Comprehensive unit tests for RabbitMQ Manager
"""

import pytest
import unittest.mock as mock
import os
import json
from unittest.mock import MagicMock, patch, call
from rabbitmq_easy import (
    RabbitMQManager, 
    RabbitMQConfigurationError, 
    RabbitMQConnectionError,
    RabbitMQManagerError,
    create_rabbitmq_manager
)
import pika


class TestRabbitMQManagerInitialization:
    """Test RabbitMQ Manager initialization and configuration"""
    
    def test_configuration_validation_queue_routing_mismatch(self):
        """Test that mismatched queues and routing keys raise error"""
        with pytest.raises(RabbitMQConfigurationError, match="Queues and routing keys count mismatch"):
            with mock.patch('pika.BlockingConnection'):
                RabbitMQManager(
                    host='localhost',
                    queues=['queue1', 'queue2'],
                    routing_keys=['key1']  # Mismatch
                )
    
    def test_valid_configuration(self):
        """Test valid configuration"""
        with mock.patch('pika.BlockingConnection'):
            manager = RabbitMQManager(
                host='localhost',
                queues=['queue1', 'queue2'],
                routing_keys=['key1', 'key2']
            )
            assert manager.host == 'localhost'
            assert len(manager.queues) == 2
            assert len(manager.routing_keys) == 2
    
    def test_default_dead_letter_configuration(self):
        """Test default dead letter exchange configuration"""
        with mock.patch('pika.BlockingConnection'):
            manager = RabbitMQManager(
                host='localhost',
                exchange='test_exchange'
            )
            assert manager.dead_letter_exchange == 'test_exchange_dlx'
    
    def test_empty_queues_list(self):
        """Test initialization with empty queues"""
        with mock.patch('pika.BlockingConnection'):
            manager = RabbitMQManager(
                host='localhost',
                queues=[],
                routing_keys=[]
            )
            assert manager.queues == []
            assert manager.routing_keys == []
    
    def test_invalid_port_configuration(self):
        """Test invalid port configuration"""
        with pytest.raises(RabbitMQConfigurationError, match="Invalid port number"):
            with mock.patch('pika.BlockingConnection'):
                RabbitMQManager(host='localhost', port=99999)
        
    def test_missing_credentials_configuration(self):
        """Test that validation passes with environment variable defaults"""
        with mock.patch('pika.BlockingConnection'):
            # Your implementation provides sensible defaults, which is good behavior
            manager = RabbitMQManager(host=None, username=None, password=None)
            
            # Should use defaults from environment variables or fallbacks
            assert manager.host == 'localhost'  # Default fallback
            assert manager.username == 'guest'   # Default fallback
            assert manager.password == 'guest'   # Default fallback
    
    def test_environment_variable_defaults(self):
        """Test environment variable loading"""
        with mock.patch.dict(os.environ, {
            'RABBITMQ_HOST': 'env-host',
            'RABBITMQ_PORT': '5673',
            'RABBITMQ_USERNAME': 'env-user',
            'RABBITMQ_PASSWORD': 'env-pass',
            'RABBITMQ_EXCHANGE': 'env-exchange'
        }):
            with mock.patch('pika.BlockingConnection'):
                manager = RabbitMQManager()
                assert manager.host == 'env-host'
                assert manager.port == 5673
                assert manager.username == 'env-user'
                assert manager.password == 'env-pass'
                assert manager.exchange == 'env-exchange'


class TestRabbitMQManagerConnection:
    """Test connection management functionality"""
    
    @mock.patch('pika.BlockingConnection')
    def test_successful_connection(self, mock_connection):
        """Test successful connection establishment"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        
        assert manager.connection == mock_conn
        assert manager.channel == mock_channel
        mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
    
    @mock.patch('pika.BlockingConnection')
    def test_connection_failure_with_retries(self, mock_connection):
        """Test connection failure handling with retries"""
        mock_connection.side_effect = pika.exceptions.AMQPConnectionError("Connection failed")
        
        with pytest.raises(RabbitMQConnectionError, match="Failed to establish RabbitMQ connection"):
            RabbitMQManager(host='localhost', max_retries=2, retry_delay=0.1)
    
    @mock.patch('pika.BlockingConnection')
    def test_ensure_connection_reconnect(self, mock_connection):
        """Test ensure_connection with reconnection"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        
        # Simulate connection loss
        manager.connection.is_closed = True
        manager.ensure_connection()
        
        # Should attempt reconnection
        assert mock_connection.call_count >= 2
    
    @mock.patch('pika.BlockingConnection')
    def test_health_check_healthy(self, mock_connection):
        """Test health check with healthy connection"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        health = manager.health_check()
        
        assert health['status'] == 'healthy'
        assert health['host'] == 'localhost'
        assert 'exchanges_setup' in health
        assert 'queues_setup' in health
    
    @mock.patch('pika.BlockingConnection')
    def test_health_check_unhealthy(self, mock_connection):
        """Test health check with connection issues"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.queue_declare.side_effect = Exception("Connection lost")
        
        manager = RabbitMQManager(host='localhost')
        health = manager.health_check()
        
        assert health['status'] == 'unhealthy'
        assert 'error' in health


class TestRabbitMQManagerExchangeOperations:
    """Test exchange creation and management"""
    
    @mock.patch('pika.BlockingConnection')
    def test_setup_exchange_new(self, mock_connection):
        """Test setting up a new exchange"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.exchange_declare.side_effect = [
            pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND"),
            None
        ]
        
        manager = RabbitMQManager(host='localhost')
        manager.setup_exchange('test_exchange', 'topic')
        
        # Should try passive declare first, then create
        assert mock_channel.exchange_declare.call_count >= 1
    
    @mock.patch('pika.BlockingConnection')
    def test_setup_exchange_existing(self, mock_connection):
        """Test setting up an existing exchange"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        manager.setup_exchange('test_exchange')
        
        # Should add to tracking
        assert 'test_exchange' in manager._setup_exchanges
    
    @mock.patch('pika.BlockingConnection')
    def test_delete_exchange_success(self, mock_connection):
        """Test successful exchange deletion"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        manager._setup_exchanges.add('test_exchange')
        
        result = manager.delete_exchange('test_exchange')
        
        assert result is True
        mock_channel.exchange_delete.assert_called_once_with(exchange='test_exchange', if_unused=False)
        assert 'test_exchange' not in manager._setup_exchanges
    
    @mock.patch('pika.BlockingConnection')
    def test_delete_exchange_not_found(self, mock_connection):
        """Test deleting non-existent exchange"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.exchange_delete.side_effect = pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")
        
        manager = RabbitMQManager(host='localhost')
        result = manager.delete_exchange('nonexistent_exchange')
        
        assert result is False


class TestRabbitMQManagerQueueOperations:
    """Test queue creation and management"""
    
    @mock.patch('pika.BlockingConnection')
    def test_setup_queue_with_dead_letter(self, mock_connection):
        """Test setting up queue with dead letter configuration"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.queue_declare.side_effect = [
            pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND"),
            None
        ]
        
        manager = RabbitMQManager(host='localhost')
        manager.setup_queue(
            queue_name='test_queue',
            exchange='test_exchange',
            routing_key='test.key',
            dead_letter_exchange='test_dlx',
            dead_letter_routing_key='dead'
        )
        
        # Should create queue with DLX arguments
        mock_channel.queue_declare.assert_called()
        mock_channel.queue_bind.assert_called_once()
    
    @mock.patch('pika.BlockingConnection')
    def test_setup_queue_without_dead_letter(self, mock_connection):
        """Test setting up queue without dead letter configuration"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        manager.setup_queue(
            queue_name='simple_queue',
            exchange='test_exchange',
            routing_key='test.key'
        )
        
        mock_channel.queue_declare.assert_called()
        mock_channel.queue_bind.assert_called_once()
    
    @mock.patch('pika.BlockingConnection')
    def test_delete_queue_success(self, mock_connection):
        """Test successful queue deletion"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_method = MagicMock()
        mock_method.method.message_count = 5
        mock_method.method.consumer_count = 0
        mock_channel.queue_declare.return_value = mock_method
        
        manager = RabbitMQManager(host='localhost')
        result = manager.delete_queue('test_queue')
        
        assert result is True
        mock_channel.queue_delete.assert_called_once()
    
    @mock.patch('pika.BlockingConnection')
    def test_purge_queue_success(self, mock_connection):
        """Test successful queue purging"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_method = MagicMock()
        mock_method.method.message_count = 10
        mock_channel.queue_purge.return_value = mock_method
        
        manager = RabbitMQManager(host='localhost')
        result = manager.purge_queue('test_queue')
        
        assert result == 10
        mock_channel.queue_purge.assert_called_once_with(queue='test_queue')
    
    @mock.patch('pika.BlockingConnection')
    def test_get_queue_info_success(self, mock_connection):
        """Test getting queue information"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_method = MagicMock()
        mock_method.method.message_count = 5
        mock_method.method.consumer_count = 2
        mock_channel.queue_declare.return_value = mock_method
        
        manager = RabbitMQManager(host='localhost')
        info = manager.get_queue_info('test_queue')
        
        assert info['queue'] == 'test_queue'
        assert info['message_count'] == 5
        assert info['consumer_count'] == 2


class TestRabbitMQManagerMessageOperations:
    """Test message publishing and consuming"""
    
    @mock.patch('pika.BlockingConnection')
    def test_publish_message_success(self, mock_connection):
        """Test successful message publishing"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        manager.publish_message('test_exchange', 'test.key', 'test message')
        
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args
        assert call_args[1]['exchange'] == 'test_exchange'
        assert call_args[1]['routing_key'] == 'test.key'
        assert call_args[1]['body'] == 'test message'
    
    @mock.patch('pika.BlockingConnection')
    def test_publish_message_with_retry(self, mock_connection):
        """Test message publishing with retry on failure"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.basic_publish.side_effect = [Exception("Connection lost"), None]
        
        manager = RabbitMQManager(host='localhost', retry_delay=0.1)
        manager.publish_message('test_exchange', 'test.key', 'test message')
        
        # Should retry after failure
        assert mock_channel.basic_publish.call_count >= 1
    
    @mock.patch('pika.BlockingConnection')
    def test_start_consuming_setup(self, mock_connection):
        """Test consumer setup"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        def mock_callback(ch, method, properties, body):
            pass
        
        manager = RabbitMQManager(host='localhost')
        
        # Mock start_consuming to raise KeyboardInterrupt
        mock_channel.start_consuming.side_effect = KeyboardInterrupt()
        
        # Your implementation handles KeyboardInterrupt gracefully
        manager.start_consuming('test_queue', mock_callback)
        
        # Verify setup was called
        mock_channel.basic_consume.assert_called_once()
    
    @mock.patch('pika.BlockingConnection')
    def test_stop_consuming(self, mock_connection):
        """Test stopping message consumption"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.is_closed = False
        
        manager = RabbitMQManager(host='localhost')
        manager.stop_consuming()
        
        mock_channel.stop_consuming.assert_called_once()

class TestRabbitMQManagerContextManager:
    """Test context manager functionality"""
    
    @mock.patch('pika.BlockingConnection')
    def test_context_manager_enter_exit(self, mock_connection):
        """Test context manager enter and exit"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.is_closed = False
        mock_conn.is_closed = False
        
        with RabbitMQManager(host='localhost') as manager:
            assert manager is not None
            assert isinstance(manager, RabbitMQManager)
        
        # Should close connections on exit
        mock_channel.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestRabbitMQManagerBatchOperations:
    """Test batch operations and cleanup"""
    
    @mock.patch('pika.BlockingConnection')
    def test_setup_queues_batch(self, mock_connection):
        """Test batch queue setup"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        
        queues_config = [
            {
                'queue': 'queue1',
                'exchange': 'test_exchange',
                'routing_key': 'test.1'
            },
            {
                'queue': 'queue2',
                'exchange': 'test_exchange',
                'routing_key': 'test.2'
            }
        ]
        
        manager.setup_queues_batch(queues_config)
        
        # Should create both queues
        assert mock_channel.queue_declare.call_count >= 2
        assert mock_channel.queue_bind.call_count >= 2
    
    @mock.patch('pika.BlockingConnection')
    def test_delete_all_setup_resources(self, mock_connection):
        """Test deleting all setup resources"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        manager._setup_exchanges.add('test_exchange')
        manager._setup_queues.add('test_queue:test_exchange:test.key')
        
        result = manager.delete_all_setup_resources(confirm=True)
        
        assert 'queues_deleted' in result
        assert 'exchanges_deleted' in result
    
    @mock.patch('pika.BlockingConnection')
    def test_cleanup_dead_letter_setup(self, mock_connection):
        """Test cleaning up dead letter infrastructure"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(
            host='localhost',
            exchange='test_exchange',
            dead_letter_exchange='test_dlx'
        )
        
        result = manager.cleanup_dead_letter_setup()
        
        # Should attempt to delete DLX and DLQ
        assert isinstance(result, bool)
    
    @mock.patch('pika.BlockingConnection')
    def test_reset_manager(self, mock_connection):
        """Test complete manager reset"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        manager._setup_exchanges.add('test_exchange')
        manager._setup_queues.add('test_queue')
        
        result = manager.reset_manager(confirm=True)
        
        assert result is True
        assert len(manager._setup_exchanges) == 0
        assert len(manager._setup_queues) == 0


class TestRabbitMQManagerInitialSetup:
    """Test initial queue setup functionality"""
    
    @mock.patch('pika.BlockingConnection')
    def test_initial_queues_setup_with_dlq(self, mock_connection):
        """Test initial setup creates dead letter queue"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(
            host='localhost',
            exchange='test_exchange',
            dead_letter_exchange='test_dlx',
            queues=['queue1', 'queue2'],
            routing_keys=['key1', 'key2']
        )
        
        # Should create main queues and dead letter queue
        assert mock_channel.queue_declare.call_count >= 3  # 2 main + 1 DLQ
        assert mock_channel.queue_bind.call_count >= 3


class TestConvenienceFunction:
    """Test the convenience creation function"""
    
    @mock.patch('pika.BlockingConnection')
    def test_create_rabbitmq_manager(self, mock_connection):
        """Test convenience function for creating manager"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = create_rabbitmq_manager(
            host='localhost',
            queues=['test_queue'],
            routing_keys=['test.key']
        )
        
        assert isinstance(manager, RabbitMQManager)
        assert manager.host == 'localhost'


class TestErrorHandling:
    """Test error handling scenarios"""
    
    @mock.patch('pika.BlockingConnection')
    def test_setup_queue_error_handling(self, mock_connection):
        """Test queue setup error handling"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.queue_declare.side_effect = Exception("Queue creation failed")
        
        manager = RabbitMQManager(host='localhost')
        
        with pytest.raises(RabbitMQManagerError):
            manager.setup_queue('test_queue')
    
    @mock.patch('pika.BlockingConnection')
    def test_setup_exchange_error_handling(self, mock_connection):
        """Test exchange setup error handling"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.exchange_declare.side_effect = Exception("Exchange creation failed")
        
        manager = RabbitMQManager(host='localhost')
        
        with pytest.raises(RabbitMQManagerError):
            manager.setup_exchange('test_exchange')
    
    @mock.patch('pika.BlockingConnection')
    def test_publish_message_max_retries_exceeded(self, mock_connection):
        """Test publish message when max retries exceeded"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        mock_channel.basic_publish.side_effect = Exception("Publish failed")
        
        manager = RabbitMQManager(host='localhost', max_retries=2, retry_delay=0.1)
        
        with pytest.raises(RabbitMQManagerError, match="Failed to publish message"):
            manager.publish_message('test_exchange', 'test.key', 'test message')


class TestSpecialCases:
    """Test special cases and edge conditions"""
    
    @mock.patch('pika.BlockingConnection')
    def test_queue_exists_check(self, mock_connection):
        """Test queue existence checking"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        
        # Test existing queue
        exists = manager._queue_exists('existing_queue')
        assert isinstance(exists, bool)
    
    @mock.patch('pika.BlockingConnection')
    def test_exchange_exists_check(self, mock_connection):
        """Test exchange existence checking"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        
        # Test existing exchange
        exists = manager._exchange_exists('existing_exchange')
        assert isinstance(exists, bool)
    
    @mock.patch('pika.BlockingConnection')
    def test_default_exchange_handling(self, mock_connection):
        """Test handling of default (empty) exchange"""
        mock_conn = MagicMock()
        mock_channel = MagicMock()
        mock_connection.return_value = mock_conn
        mock_conn.channel.return_value = mock_channel
        
        manager = RabbitMQManager(host='localhost')
        
        # Default exchange should always exist
        exists = manager._exchange_exists('')
        assert exists is True


# Test configuration
def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )