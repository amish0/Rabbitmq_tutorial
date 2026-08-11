"""Example 7: RPC-style request/response using correlation_id and reply_to."""
from .rabbitmq_helper import make_connection, RpcClient

EX = 'rpc_ex'
ROUTING = 'rpc'


def rpc_server():
    conn = make_connection()
    ch = conn.channel()
    ch.exchange_declare(exchange=EX, exchange_type='direct', durable=True)
    q = ch.queue_declare(queue='rpc_queue', durable=True)
    ch.queue_bind(queue='rpc_queue', exchange=EX, routing_key=ROUTING)

    def on_request(ch, method, props, body):
        req = body.decode()
        print('RPC request', req)
        response = f'echo:{req}'
        ch.basic_publish(exchange='', routing_key=props.reply_to, properties=None, body=response.encode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue='rpc_queue', on_message_callback=on_request)
    ch.start_consuming()


def rpc_client_call(message):
    conn = make_connection()
    client = RpcClient(conn)
    resp = client.call(exchange=EX, routing_key=ROUTING, body=message.encode())
    print('RPC response', resp)
    conn.close()
