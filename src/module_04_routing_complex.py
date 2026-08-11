"""Example 4: Complex routing and message flow between producers, exchanges and consumers.
Includes consumer modifying message and republishing to other exchange.
"""
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EX_A = 'exchange_A'
EX_B = 'exchange_B'
QUEUE_A = 'queue_A'
QUEUE_B = 'queue_B'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EX_A, exchange_type='direct', durable=True)
    declare_exchange(ch, EX_B, exchange_type='direct', durable=True)
    declare_queue(ch, QUEUE_A, durable=True)
    declare_queue(ch, QUEUE_B, durable=True)
    ch.queue_bind(queue=QUEUE_A, exchange=EX_A, routing_key='rk.A')
    ch.queue_bind(queue=QUEUE_B, exchange=EX_A, routing_key='rk.B')
    ch.queue_bind(queue=QUEUE_B, exchange=EX_B, routing_key='rk.fromB')
    conn.close()


def producer_a():
    conn = make_connection()
    ch = conn.channel()
    for i in range(5):
        publish(ch, EX_A, 'rk.A', f'A-{i}')
        publish(ch, EX_A, 'rk.B', f'B-{i}')
        print('Producer A sent A-', i, 'and B-', i)
        time.sleep(0.1)
    conn.close()


def consumer_b_modify_and_repub():
    conn = make_connection()
    ch = conn.channel()

    def cb(ch, method, props, body):
        data = body.decode()
        new = data + '::modified'
        # republish to exchange B
        publish(ch, EX_B, 'rk.fromB', new)
        print('Consumer B modified and republished', new)
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE_B, on_message_callback=cb)
    ch.start_consuming()


def consumer_a():
    conn = make_connection()
    ch = conn.channel()

    def cb(ch, method, props, body):
        print('Consumer A got', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE_A, on_message_callback=cb)
    ch.start_consuming()
