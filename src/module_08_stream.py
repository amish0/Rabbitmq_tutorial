"""Example 8: Stream-like behaviour using persistent queues and prefetching.
This is a simplified demo using regular queues to mimic stream consumption semantics.
"""
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EX = 'stream_ex'
Q = 'stream_q'


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EX, exchange_type='direct', durable=True)
    declare_queue(ch, Q, durable=True)
    ch.queue_bind(queue=Q, exchange=EX, routing_key='s')
    for i in range(20):
        publish(ch, EX, 's', f'stream-{i}')
        print('Produced', i)
        time.sleep(0.05)
    conn.close()


def consumer(batch_size=10):
    conn = make_connection()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=batch_size)

    def cb(ch, method, props, body):
        print('Stream received', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=Q, on_message_callback=cb)
    ch.start_consuming()
