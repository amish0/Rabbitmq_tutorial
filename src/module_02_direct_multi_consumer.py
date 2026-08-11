"""Example 2: One producer with multiple consumers for direct exchange.
Demonstrates same routing key vs different routing key and durability.
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'direct_multi'
QUEUE_A = 'direct_multi_A'
QUEUE_B = 'direct_multi_B'


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    # send messages with two routing keys
    for i in range(5):
        publish(ch, EXCHANGE, 'key.a', f'a-{i}')
        publish(ch, EXCHANGE, 'key.b', f'b-{i}')
        print('Sent a-', i, 'b-', i)
        time.sleep(0.1)
    conn.close()


def consume_a():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    declare_queue(ch, QUEUE_A, durable=True)
    ch.queue_bind(queue=QUEUE_A, exchange=EXCHANGE, routing_key='key.a')

    def cb(ch, method, props, body):
        print('A got', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE_A, on_message_callback=cb)
    ch.start_consuming()


def consume_b():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    declare_queue(ch, QUEUE_B, durable=True)
    ch.queue_bind(queue=QUEUE_B, exchange=EXCHANGE, routing_key='key.b')

    def cb(ch, method, props, body):
        print('B got', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE_B, on_message_callback=cb)
    ch.start_consuming()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: produce|consume_a|consume_b')
    elif sys.argv[1] == 'produce':
        produce()
    elif sys.argv[1] == 'consume_a':
        consume_a()
    else:
        consume_b()
