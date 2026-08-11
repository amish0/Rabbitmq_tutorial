"""Example 13: Streaming-style batch consumption with prefetch.
Run producer: python -m src.module_13_stream_batch produce
Run consumer: python -m src.module_13_stream_batch consume
"""
import sys
import time
from .rabbitmq_helper import make_connection, declare_exchange, declare_queue, publish

EXCHANGE = 'stream_batch_ex'
QUEUE = 'stream_batch_q'


def setup():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    declare_queue(ch, QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key='stream')
    conn.close()


def produce():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    for i in range(20):
        publish(ch, EXCHANGE, 'stream', f'stream-msg-{i}')
        print('Produced', i)
        time.sleep(0.05)
    conn.close()


def consume():
    conn = make_connection()
    ch = conn.channel()
    ch.basic_qos(prefetch_count=5)

    def callback(ch, method, properties, body):
        print('Consumed', body.decode())
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=QUEUE, on_message_callback=callback)
    print('Waiting for stream messages...')
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