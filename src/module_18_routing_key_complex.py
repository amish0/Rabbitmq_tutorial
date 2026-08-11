"""Example 18: Complex routing with multiple binding rules.
Run setup: python -m src.module_18_routing_key_complex setup
Run producer: python -m src.module_18_routing_key_complex produce
Run consumer: python -m src.module_18_routing_key_complex consume queue_a|queue_b
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'routing_key_complex_ex'
QUEUE_A = 'routing_a_q'
QUEUE_B = 'routing_b_q'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    declare_queue(ch, QUEUE_A, durable=True)
    declare_queue(ch, QUEUE_B, durable=True)
    ch.queue_bind(queue=QUEUE_A, exchange=EXCHANGE, routing_key='alpha')
    ch.queue_bind(queue=QUEUE_B, exchange=EXCHANGE, routing_key='beta')
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    publish(ch, EXCHANGE, 'alpha', 'alpha-message')
    publish(ch, EXCHANGE, 'beta', 'beta-message')
    publish(ch, EXCHANGE, 'gamma', 'gamma-message')
    print('Published alpha, beta, gamma')
    conn.close()


def consume(queue_name):
    conn = make_connection()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        print(queue_name, 'received', method.routing_key, body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    if queue_name == QUEUE_A:
        queue = QUEUE_A
    else:
        queue = QUEUE_B

    ch.basic_consume(queue=queue, on_message_callback=callback)
    print('Waiting on', queue)
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
    elif sys.argv[1] == 'consume' and len(sys.argv) == 2:
        consume(sys.argv[1])
    else:
        print('usage: setup|produce|consume queue_name')