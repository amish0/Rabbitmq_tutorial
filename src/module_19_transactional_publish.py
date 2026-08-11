"""Example 19: Transactional publish to ensure all-or-nothing delivery.
Run producer: python -m src.module_19_transactional_publish produce
"""
import pika
from .rabbitmq_helper import make_connection, declare_exchange

EXCHANGE = 'txn_ex'


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    ch.tx_select()
    try:
        ch.basic_publish(exchange=EXCHANGE, routing_key='txn', body='txn-1')
        ch.basic_publish(exchange=EXCHANGE, routing_key='txn', body='txn-2')
        ch.tx_commit()
        print('Transaction committed')
    except Exception as exc:
        ch.tx_rollback()
        print('Transaction rolled back', exc)
    finally:
        conn.close()


if __name__ == '__main__':
    produce()