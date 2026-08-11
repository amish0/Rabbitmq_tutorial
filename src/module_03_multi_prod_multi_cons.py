"""Example 3: Multiple producers and multiple consumers sharing one exchange.
Messages use different routing keys to route to different queues.
"""
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'multi_pp_cc'


def producer(name, routing_key, count=5):
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    for i in range(count):
        msg = f'{name}-{i}'
        publish(ch, EXCHANGE, routing_key, msg)
        print('Sent', msg, '->', routing_key)
        time.sleep(0.1)
    conn.close()


def consumer(queue, routing_key):
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    declare_queue(ch, queue, durable=True)
    ch.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=routing_key)

    def cb(ch, method, props, body):
        print(queue, 'received', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=queue, on_message_callback=cb)
    ch.start_consuming()
