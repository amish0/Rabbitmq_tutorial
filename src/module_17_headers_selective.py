"""Example 17: Headers exchange with selective consumer binding.
Run setup: python -m src.module_17_headers_selective setup
Run producer: python -m src.module_17_headers_selective produce
Run consumer: python -m src.module_17_headers_selective consume
"""
import sys
import time
import pika
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'headers_ex'
QUEUE = 'headers_q'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='headers', durable=True)
    args = {'x-match': 'all', 'format': 'pdf', 'type': 'report'}
    declare_queue(ch, QUEUE, durable=True, arguments=args)
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    headers = {'format': 'pdf', 'type': 'report'}
    props = pika.BasicProperties(headers=headers)
    publish(ch, EXCHANGE, '', 'report-pdf', properties=props)
    print('Sent headers message')
    conn.close()


def consume():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='headers', durable=True)
    declare_queue(ch, QUEUE, durable=True, arguments={'x-match': 'all', 'format': 'pdf', 'type': 'report'})

    def callback(ch, method, properties, body):
        print('Received headers message', body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=QUEUE, on_message_callback=callback)
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