"""Example 10: Topic exchange with wildcard bindings.
Run setup: python -m src.module_10_topic_wildcard setup
Run producer: python -m src.module_10_topic_wildcard produce
Run consumer: python -m src.module_10_topic_wildcard payment|created
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'topic_wildcard_ex'
PAYMENT_QUEUE = 'topic_payment_q'
CREATED_QUEUE = 'topic_created_q'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='topic', durable=True)
    declare_queue(ch, PAYMENT_QUEUE, durable=True)
    declare_queue(ch, CREATED_QUEUE, durable=True)
    ch.queue_bind(queue=PAYMENT_QUEUE, exchange=EXCHANGE, routing_key='payment.#')
    ch.queue_bind(queue=CREATED_QUEUE, exchange=EXCHANGE, routing_key='*.created')
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='topic', durable=True)
    messages = [
        ('payment.success', 'payment.success:123'),
        ('payment.failure', 'payment.failure:456'),
        ('order.created', 'order.created:789'),
        ('order.cancelled', 'order.cancelled:999'),
    ]
    for routing_key, body in messages:
        publish(ch, EXCHANGE, routing_key, body)
        print('Sent', routing_key, body)
        time.sleep(0.1)
    conn.close()


def consume(queue_name):
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='topic', durable=True)
    declare_queue(ch, queue_name, durable=True)

    def callback(ch, method, properties, body):
        print(queue_name, 'received', method.routing_key, body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=queue_name, on_message_callback=callback)
    print('Waiting for', queue_name, 'messages...')
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: setup|produce|payment|created')
    elif sys.argv[1] == 'setup':
        setup()
    elif sys.argv[1] == 'produce':
        produce()
    elif sys.argv[1] == 'payment':
        consume(PAYMENT_QUEUE)
    elif sys.argv[1] == 'created':
        consume(CREATED_QUEUE)
    else:
        print('unknown command')