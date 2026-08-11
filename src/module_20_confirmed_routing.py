"""Example 20: Confirmed publish with publisher acknowledgements and routing confirmation.
Run producer: python -m src.module_20_confirmed_routing produce
"""
import pika
from .rabbitmq_helper import make_connection, declare_exchange

EXCHANGE = 'confirm_route_ex'


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    ch.confirm_delivery()
    success = ch.basic_publish(exchange=EXCHANGE, routing_key='confirm', body='route-confirm')
    print('Publish success:', success)
    conn.close()


if __name__ == '__main__':
    produce()