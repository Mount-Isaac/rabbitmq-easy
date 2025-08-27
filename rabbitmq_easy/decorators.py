import functools
from typing import Callable, Any, Optional
import asyncio
import inspect
import json


def queue_consumer(queue: str, exchange: Optional[str]=None, auto_ack:bool=True):
    '''Universal message handler decorator that works with any callback function'''
    def decorator(func:Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(ch, method, properties, body):
            try:
                try:
                    message = json.loads(body)
                except json.JSONDecodeError:
                    message = body.decode('utf8')

                # check if function is async 
                if asyncio.iscoroutinefunction(func):
                    loop = asyncio.get_event_loop()
                    result = loop.run_until_complete(func(message))
                else:
                    result = func(message)
                
                if auto_ack:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                return result
            except Exception as e:
                print(f"Error processing message: {str()}")
                if auto_ack:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
                raise
        wrapper._quueu= queue
        wrapper._exchange = exchange
        wrapper._original_func = func

        return wrapper
        
    return decorator