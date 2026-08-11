"""Simple RabbitMQ helpers using pika BlockingConnection.
Provides connection, publish, and consume helpers for examples.
"""
import pika
import uuid


def make_connection(url='amqp://guest:guest@localhost:5672/%2F'):
    params = pika.URLParameters(url)
    return pika.BlockingConnection(params)


def publish(channel, exchange, routing_key, body, properties=None, mandatory=False):
    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties, mandatory=mandatory)


def declare_queue(channel, queue, durable=False, arguments=None):
    channel.queue_declare(queue=queue, durable=durable, arguments=arguments)


def declare_exchange(channel, exchange, exchange_type='direct', durable=False, arguments=None):
    channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=durable, arguments=arguments)


def basic_consume(channel, queue, on_message_callback, auto_ack=False):
    channel.basic_consume(queue=queue, on_message_callback=on_message_callback, auto_ack=auto_ack)


def start_consuming(connection):
    try:
        connection.process_data_events(time_limit=0)
        connection.channel().start_consuming()
    except KeyboardInterrupt:
        try:
            connection.close()
        except Exception:
            pass


class RpcClient:
    def __init__(self, connection):
        self.connection = connection
        self.channel = connection.channel()
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        self.response = None
        self.corr_id = None
        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, exchange, routing_key, body, timeout=5):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        props = pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id)
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, properties=props, body=body)
        # wait for response
        import time
        deadline = time.time() + timeout
        while self.response is None and time.time() < deadline:
            self.connection.process_data_events(time_limit=0.1)
        return self.response
