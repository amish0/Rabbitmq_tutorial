"""Run example modules by module name.
Usage: python -m src.run_module module_01_direct_single produce
"""
import sys
import importlib

if len(sys.argv) < 2:
    print('usage: <module> [args]')
    sys.exit(1)

mod_name = sys.argv[1]
args = sys.argv[2:]
try:
    mod = importlib.import_module('src.' + mod_name)
except Exception as e:
    print('Cannot import', mod_name, e)
    sys.exit(2)

if hasattr(mod, 'main'):
    mod.main(*args)
else:
    # fallback: try calling produce/consume based on args
    if args:
        fn = args[0]
        if hasattr(mod, fn):
            getattr(mod, fn)(*args[1:])
        else:
            print('Module has no function', fn)
    else:
        print('No function specified for module', mod_name)
