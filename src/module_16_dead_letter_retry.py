"""Example 16: Dead-letter retry pattern with immediate retry via DLX.
Run setup: python -m src.module_16_dead_letter_retry setup
Run producer: python -m src.module_16_dead_letter_retry produce
Run consumer: python -m src.module_16_dead_letter_retry consume
Run dlq consumer: python -m src.module_16_dead_letter_retry dlq
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

MAIN_EX = 'retry_main_ex'
MAIN_Q = 'retry_main_q'
DLX = 'retry_dlx'
DLQ = 'retry_dlq'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, DLX, exchange_type='direct', durable=True)
    declare_queue(ch, DLQ, durable=True)
    ch.queue_bind(queue=DLQ, exchange=DLX, routing_key='retry')
    args = {'x-dead-letter-exchange': DLX, 'x-dead-letter-routing-key': 'retry'}
    declare_exchange(ch, MAIN_EX, exchange_type='direct', durable=True)
    declare_queue(ch, MAIN_Q, durable=True, arguments=args)
    ch.queue_bind(queue=MAIN_Q, exchange=MAIN_EX, routing_key='task')
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    for i in range(3):
        publish(ch, MAIN_EX, 'task', f'retry-{i}')
        print('Produced retry-', i)
        time.sleep(0.1)
    conn.close()


def consume():
    conn = make_connection()
    ch = conn.channel()

    def callback(ch, method, properties, body):
        print('Rejecting', body.decode())
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    ch.basic_consume(queue=MAIN_Q, on_message_callback=callback)
    ch.start_consuming()


def dlq():
    conn = make_connection()
    ch = conn.channel()

    def callback(ch, method, properties, body):
        print('DLQ received', body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=DLQ, on_message_callback=callback)
    ch.start_consuming()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: setup|produce|consume|dlq')
    elif sys.argv[1] == 'setup':
        setup()
    elif sys.argv[1] == 'produce':
        produce()
    elif sys.argv[1] == 'consume':
        consume()
    elif sys.argv[1] == 'dlq':
        dlq()
    else:
        print('usage: setup|produce|consume|dlq')