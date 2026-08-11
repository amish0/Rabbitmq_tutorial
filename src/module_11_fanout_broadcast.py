"""Example 11: Fanout broadcast with multiple consumers and durable queues.
Run setup: python -m src.module_11_fanout_broadcast setup
Run producer: python -m src.module_11_fanout_broadcast produce
Run consumer: python -m src.module_11_fanout_broadcast consume queue_a|queue_b
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'fanout_broadcast_ex'
QUEUE_A = 'fanout_q_a'
QUEUE_B = 'fanout_q_b'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='fanout', durable=True)
    declare_queue(ch, QUEUE_A, durable=True)
    declare_queue(ch, QUEUE_B, durable=True)
    ch.queue_bind(queue=QUEUE_A, exchange=EXCHANGE)
    ch.queue_bind(queue=QUEUE_B, exchange=EXCHANGE)
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='fanout', durable=True)
    for i in range(5):
        publish(ch, EXCHANGE, '', f'broadcast-{i}')
        print('Broadcasted', i)
        time.sleep(0.1)
    conn.close()


def consume(queue_name):
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='fanout', durable=True)
    declare_queue(ch, queue_name, durable=True)
    ch.queue_bind(queue=queue_name, exchange=EXCHANGE)

    def callback(ch, method, properties, body):
        print(queue_name, 'received', body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=queue_name, on_message_callback=callback)
    print('Waiting for fanout messages on', queue_name)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: setup|produce|consume queue_name')
    elif sys.argv[1] == 'setup':
        setup()
    elif sys.argv[1] == 'produce':
        produce()
    elif sys.argv[1] == 'consume' and len(sys.argv) == 3:
        consume(sys.argv[2])
    else:
        print('usage: setup|produce|consume queue_name')