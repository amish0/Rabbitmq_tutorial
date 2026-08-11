# RabbitMQ Advanced Complex Pipeline

This repository focuses on one advanced, production-style RabbitMQ example housed in `src/book/module_advanced_complex`.

## What is included

- Multi-producer event consolidation using `adv.exchange.A`
- Routing by semantic keys: `rk.A` and `rk.B`
- Transform-and-forward processing via `consumer_b`
- Final processed delivery through `adv.exchange.processed`
- Dead-letter capture for failures in `adv.exchange.dlx`

## Architecture overview

```mermaid
flowchart LR
    subgraph Producers
        PA[Producer A]
        PB[Producer B]
    end
    PA -->|rk.A| EXA[adv.exchange.A]
    PA -->|rk.B| EXA
    PB -->|rk.B| EXA
    EXA -->|rk.A| QA[adv.queue.A]
    EXA -->|rk.B| QB[adv.queue.B]
    QA --> CA[consumer_a]
    QB --> CB[consumer_b]
    CB -->|transform & republish| EXP[adv.exchange.processed]
    EXP -->|processed| QP[adv.queue.processed]
    QP --> CP[processed_consumer]
    QA -.reject/expire.-> DLX[adv.exchange.dlx]
    QB -.reject/expire.-> DLX
    DLX --> DLQ[adv.queue.dlx]
```

## Run the advanced example

```bash
python -m src.book.module_advanced_complex.setup
python -m src.book.module_advanced_complex.consumer_a
python -m src.book.module_advanced_complex.consumer_b
python -m src.book.module_advanced_complex.processed_consumer
python -m src.book.module_advanced_complex.producer_a produce
python -m src.book.module_advanced_complex.producer_b produce
```

## More details

See `src/book/module_advanced_complex/README.md` for the complete use case, topology labels, and implementation guidance.

## Notes

This top-level README is intentionally short and introductory. The module README contains the full advanced scenario description and commands.
