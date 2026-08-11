"""Example 12: RPC request/response pattern using correlation_id and reply_to.
Run server: python -m src.module_12_rpc_request_response server
Run client: python -m src.module_12_rpc_request_response call <message>
"""
import sys
from .rabbitmq_helper import make_connection, RpcClient

EXCHANGE = 'rpc_example_ex'
QUEUE = 'rpc_request_q'
ROUTING_KEY = 'rpc.request'


def server():
    conn = make_connection()
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type='direct', durable=True)
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    def on_request(ch, method, props, body):
        request = body.decode()
        print('RPC request', request)
        response = f'processed:{request}'
        ch.basic_publish(exchange='', routing_key=props.reply_to, body=response.encode(), properties=None)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE, on_message_callback=on_request)
    print('RPC server waiting...')
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        conn.close()


def call(message):
    conn = make_connection()
    client = RpcClient(conn)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type='direct', durable=True)
    response = client.call(exchange=EXCHANGE, routing_key=ROUTING_KEY, body=message.encode())
    print('RPC response', response.decode() if response else None)
    conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('usage: server|call <message>')
    elif sys.argv[1] == 'server':
        server()
    elif sys.argv[1] == 'call' and len(sys.argv) == 3:
        call(sys.argv[2])
    else:
        print('usage: server|call <message>')