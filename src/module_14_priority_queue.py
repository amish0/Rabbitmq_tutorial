"""Example 14: Priority queue with multiple priority levels.
Run setup: python -m src.module_14_priority_queue setup
Run producer: python -m src.module_14_priority_queue produce
Run consumer: python -m src.module_14_priority_queue consume
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'priority_ex'
QUEUE = 'priority_q'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    args = {'x-max-priority': 10}
    declare_queue(ch, QUEUE, durable=True, arguments=args)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key='priority')
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    for priority, body in [(1, 'low'), (5, 'medium'), (9, 'high')]:
        props = pika.BasicProperties(priority=priority)
        publish(ch, EXCHANGE, 'priority', body, properties=props)
        print('Sent', body, 'priority', priority)
        time.sleep(0.1)
    conn.close()


def consume():
    conn = make_connection()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        print('Received', body.decode(), 'priority', properties.priority)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=QUEUE, on_message_callback=callback)
    print('Waiting for priority queue messages...')
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: setup|produce|consume')
    elif sys.argv[1] == 'setup':
        setup()
    elif sys.argv[1] == 'produce':
        produce()
    else:
        consume()