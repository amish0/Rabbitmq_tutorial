"""Generator to create a tutorial 'book' of modules (1..50).

Each module will be placed under `src/book/module_XX/` and contain:
 - `producer.py` (example producer)
 - `consumer.py` (example consumer)
 - `submodule_a.py`, `submodule_b.py` (variants)
 - `README.md` with a mermaid diagram

Run: python -m src.generate_modules
"""
import os
BASE = os.path.dirname(__file__)
OUT_DIR = os.path.join(BASE, 'book')
os.makedirs(OUT_DIR, exist_ok=True)

PRODUCER_TMPL = '''from ..rabbitmq_helper import make_connection, declare_exchange, publish

EXCHANGE = "{exchange}"
ROUTING_KEY = "{rk}"

def produce(count=5):
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    for i in range(count):
        msg = f"{mod_name}-{{i}}"
        publish(ch, EXCHANGE, ROUTING_KEY, msg)
        print('Produced', msg)
    conn.close()
'''

CONSUMER_TMPL = '''from ..rabbitmq_helper import make_connection, declare_exchange, declare_queue

EXCHANGE = "{exchange}"
QUEUE = "{queue}"
ROUTING_KEY = "{rk}"

def consume():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable=True)
    declare_queue(ch, QUEUE, durable=True)
    ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)

    def cb(ch, method, props, body):
        print('Consumed', body.decode())
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(queue=QUEUE, on_message_callback=cb)
    ch.start_consuming()
'''

SUB_TMPL = '''from ..rabbitmq_helper import make_connection, declare_exchange, publish

EXCHANGE = "{exchange}"
RK = "{rk}"

def run_variant():
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EXCHANGE, exchange_type='direct', durable={durable})
    publish(ch, EXCHANGE, RK, 'variant-message')
    print('Sent variant-message')
    conn.close()
'''

README_TMPL = '''# Module {num:02d}: {title}

Short tutorial for module `{mod_dir}`.

```mermaid
flowchart LR
    Producer -->|{rk}| Exchange[{exchange}]
    Exchange -->|{rk}| Queue[{queue}]
    Queue --> Consumer
```

Examples:
- `producer.py` — send messages
- `consumer.py` — receive messages
- `submodule_a.py` / `submodule_b.py` — variants (durable, headers, etc.)
'''

for i in range(1, 51):
    mod_dir = f'module_{i:02d}'
    path = os.path.join(OUT_DIR, mod_dir)
    os.makedirs(path, exist_ok=True)
    exchange = f'book_ex_{i}'
    rk = f'book.rk.{i%5}'
    queue = f'{mod_dir}_q'

    # write producer
    with open(os.path.join(path, 'producer.py'), 'w', encoding='utf8') as f:
        f.write(PRODUCER_TMPL.format(exchange=exchange, rk=rk, mod_name=mod_dir))

    # write consumer
    with open(os.path.join(path, 'consumer.py'), 'w', encoding='utf8') as f:
        f.write(CONSUMER_TMPL.format(exchange=exchange, queue=queue, rk=rk))

    # submodules
    with open(os.path.join(path, 'submodule_a.py'), 'w', encoding='utf8') as f:
        f.write(SUB_TMPL.format(exchange=exchange, rk=rk, durable='True'))
    with open(os.path.join(path, 'submodule_b.py'), 'w', encoding='utf8') as f:
        f.write(SUB_TMPL.format(exchange=exchange, rk=rk, durable='False'))

    # __init__.py for package import convenience
    with open(os.path.join(path, '__init__.py'), 'w', encoding='utf8') as f:
        f.write(f"# {mod_dir} package\n")

    # README with mermaid diagram
    with open(os.path.join(path, 'README.md'), 'w', encoding='utf8') as f:
        f.write(README_TMPL.format(num=i, title=f'Tutorial {i}', mod_dir=mod_dir, exchange=exchange, rk=rk, queue=queue))

print('Created 50 tutorial modules under', OUT_DIR)
