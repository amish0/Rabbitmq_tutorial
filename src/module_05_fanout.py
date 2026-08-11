"""Example 5: Fanout exchange - broadcast to all bound queues."""
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EX = 'fanout_ex'


def producer():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EX, exchange_type='fanout', durable=True)
    for i in range(5):
        publish(ch, EX, '', f'broadcast-{i}')
        print('Broadcasted', i)
        time.sleep(0.1)
    conn.close()


def consumer(queue_name):
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EX, exchange_type='fanout', durable=True)
    declare_queue(ch, queue_name, durable=True)
    ch.queue_bind(queue=queue_name, exchange=EX)

    def cb(ch, method, props, body):
        print(queue_name, 'received', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=queue_name, on_message_callback=cb)
    ch.start_consuming()
