"""Example 15: Publisher confirms for reliable publish acknowledgement.
Run producer: python -m src.module_15_confirm_publish produce
"""
import sys
import time
import pika
from .rabbitmq_helper import make_connection, declare_exchange, publish

EXCHANGE = 'confirm_ex'


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    ch.confirm_delivery()
    for i in range(5):
        body = f'confirm-{i}'
        success = ch.basic_publish(exchange=EXCHANGE, routing_key='confirm', body=body)
        print('Published', body, 'ack=', success)
        time.sleep(0.1)
    conn.close()


if __name__ == '__main__':
    produce()