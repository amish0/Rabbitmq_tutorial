"""Example 6: Dead-letter exchange pattern.
Messages rejected or expired routed to dead-letter exchange/queue.
"""
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

DLX = 'dead_letter_ex'
DLQ = 'dead_letter_q'
MAIN_EX = 'main_with_dlx'
MAIN_Q = 'main_q'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, DLX, exchange_type='direct', durable=True)
    declare_queue(ch, DLQ, durable=True)
    ch.queue_bind(queue=DLQ, exchange=DLX, routing_key='dlx')
    args = {'x-dead-letter-exchange': DLX, 'x-dead-letter-routing-key': 'dlx'}
    declare_exchange(ch, MAIN_EX, exchange_type='direct', durable=True)
    declare_queue(ch, MAIN_Q, durable=True, arguments=args)
    ch.queue_bind(queue=MAIN_Q, exchange=MAIN_EX, routing_key='task')
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    for i in range(5):
        publish(ch, MAIN_EX, 'task', f'task-{i}')
        print('Produced', i)
        time.sleep(0.1)
    conn.close()


def consumer_and_reject():
    conn = make_connection()
    ch = conn.channel()

    def cb(ch, method, props, body):
        print('Rejecting', body.decode())
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    ch.basic_consume(queue=MAIN_Q, on_message_callback=cb)
    ch.start_consuming()


def dead_consumer():
    conn = make_connection()
    ch = conn.channel()

    def cb(ch, method, props, body):
        print('Dead-letter received', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=DLQ, on_message_callback=cb)
    ch.start_consuming()
