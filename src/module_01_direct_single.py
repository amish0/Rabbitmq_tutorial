"""Example 1: One producer and one consumer using direct exchange.
Run producer: python -m src.module_01_direct_single produce
Run consumer: python -m src.module_01_direct_single consume
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'direct_example'
QUEUE = 'direct_example_queue'
ROUTING_KEY = 'task'


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    for i in range(10):
        body = f'message-{i}'
        publish(ch, EXCHANGE, ROUTING_KEY, body)
        print('Sent', body)
        time.sleep(0.1)
    conn.close()


def consume():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    declare_queue(ch, QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    def callback(ch, method, properties, body):
        print('Received', body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE, on_message_callback=callback)
    print('Waiting for messages...')
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: produce|consume')
    elif sys.argv[1] == 'produce':
        produce()
    else:
        consume()
