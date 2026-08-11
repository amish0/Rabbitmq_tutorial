"""Generator to create additional example modules programmatically.
Run: python -m src.generate_modules
This will create numbered modules under src/generated/module_09_..._module_50.py
"""
import os

BASE = os.path.dirname(__file__)
OUT_DIR = os.path.join(BASE, 'generated')

TEMPLATE = '''"""Auto-generated module: {name}
A lightweight example showing a producer for exchange {exchange} and routing {rk}.
"""
from .rabbitmq_helper import make_connection, declare_exchange, publish

EX = '{exchange}'
RK = '{rk}'


def produce(count=3):
    conn = make_connection()
    ch = conn.channel()
    declare_exchange(ch, EX, exchange_type='direct', durable=True)
    for i in range(count):
        publish(ch, EX, RK, f"{name}-{{}}".format(i))
    conn.close()
'''

os.makedirs(OUT_DIR, exist_ok=True)
for i in range(9, 51):
    name = f'module_{i:02d}'
    filename = os.path.join(OUT_DIR, f'{name}.py')
    exchange = f'gen_ex_{i}'
    rk = f'gen.rk.{i%5}'
    with open(filename, 'w', encoding='utf8') as f:
        f.write(TEMPLATE.format(name=name, exchange=exchange, rk=rk))
print('Generated', len(os.listdir(OUT_DIR)), 'modules in', OUT_DIR)
