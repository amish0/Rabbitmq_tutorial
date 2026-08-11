# RabbitMQ Python Tutorial (Beginner → Advanced)

This repository contains hands-on RabbitMQ examples in Python using `pika`.

Structure
- `src/` — core helper and example modules
- `src/generated/` — auto-generated modules (09-50)

Quick start
1. Install RabbitMQ and ensure it's running on `localhost:5672`.
2. Create a virtualenv and install deps:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Running examples
- Basic direct exchange producer/consumer:

```bash
python -m src.module_01_direct_single produce
python -m src.module_01_direct_single consume
```

- Generate the additional modules (09-50):

```bash
python -m src.generate_modules
```

Mermaid diagrams (core modules)

**Module 01 Direct**:

```mermaid
flowchart LR
    Producer -->|routing key: task| Exchange[direct_example]
    Exchange -->|task| Queue[direct_example_queue]
    Queue --> Consumer
```

**Module 02 Multi Consumer (direct)**:

```mermaid
flowchart LR
    Producer --> Exchange[direct_multi]
    Exchange -->|key.a| QueueA[direct_multi_A]
    Exchange -->|key.b| QueueB[direct_multi_B]
    QueueA --> ConsumerA
    QueueB --> ConsumerB
```

**Module 03 Multi P->P C**:

```mermaid
flowchart LR
    ProducerA --> Exchange[multi_pp_cc]
    ProducerB --> Exchange
    Exchange -->|rk1| Queue1
    Exchange -->|rk2| Queue2
```

**Module 04 Complex routing**:

```mermaid
flowchart LR
    ProducerA -->|rk.A| ExchangeA
    ProducerA -->|rk.B| ExchangeA
    ExchangeA --> QueueA
    ExchangeA --> QueueB
    QueueB -->|modify & republish| ExchangeB
    ExchangeB --> QueueB2
```

**Module 05 Fanout**:

```mermaid
flowchart LR
    Producer --> FanoutEx
    FanoutEx --> Queue1
    FanoutEx --> Queue2
    Queue1 --> Consumer1
    Queue2 --> Consumer2
```

**Module 06 Dead Letter**:

```mermaid
flowchart LR
    Producer --> MainExchange
    MainExchange --> MainQueue
    MainQueue -.reject./expire.-> DLX[DeadLetterEx]
    DLX --> DeadQueue
```

**Module 07 RPC**:

```mermaid
flowchart LR
    Client -->|rpc req| RPCExchange
    RPCExchange --> RPCQueue
    RPCQueue --> Server
    Server -->|reply via reply_to| ClientCallbackQueue
```

**Module 08 Stream (simplified)**:

```mermaid
flowchart LR
    Producer --> StreamExchange
    StreamExchange --> StreamQueue
    StreamQueue --> Consumer(batch)
```

Generated modules (09-50)
- `src/generated/` contains programmatically generated examples covering variations in routing keys, exchanges, and durability. Run `python -m src.generate_modules` to recreate.

Notes & production tips
- Use connection pools or long-lived connections in production.
- Set `durable=True` on exchanges/queues and persistent delivery_mode=2 for messages that must survive broker restarts.
- Monitor queue lengths and set appropriate prefetch counts.
- Use Dead-Letter Exchanges for retries and failed message handling.
- For high-throughput streaming consider RabbitMQ Streams plugin.

If you want, I can:
- Run the generator now and add a short script to run arbitrary module by name.
- Expand individual modules into fully runnable Docker-compose + tests.

Files created:
- `src/rabbitmq_helper.py`
- `src/module_01_direct_single.py` ... `src/module_08_stream.py`
- `src/generate_modules.py`
- `requirements.txt`
