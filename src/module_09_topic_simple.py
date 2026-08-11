"""Example 9: Topic exchange with semantic routing keys.
Run producer: python -m src.module_09_topic_simple produce
Run consumer: python -m src.module_09_topic_simple consume
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'topic_example'
QUEUE = 'topic_order_q'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='topic', durable=True)
    declare_queue(ch, QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key='order.*')
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='topic', durable=True)
    for routing_key, body in [
        ('order.created', 'order.created:1001'),
        ('order.updated', 'order.updated:1001'),
        ('shipment.created', 'shipment.created:5001'),
    ]:
        publish(ch, EXCHANGE, routing_key, body)
        print('Sent', routing_key, body)
        time.sleep(0.1)
    conn.close()


def consume():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='topic', durable=True)
    declare_queue(ch, QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key='order.*')

    def callback(ch, method, properties, body):
        print('Received', method.routing_key, body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE, on_message_callback=callback)
    print('Waiting for topic messages...')
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