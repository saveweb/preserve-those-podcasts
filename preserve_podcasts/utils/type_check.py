import inspect
from rich import print
import sys

FORCE_RAISE_EXCEPTION = False

class runtimeTypeCheck:
    """ Decorator to check types at runtime. """

    def __init__(self, raise_exception=True):
        self.raise_exception = raise_exception or FORCE_RAISE_EXCEPTION

    def __call__(self, f):
        def wrapper(*args, **kwargs):
            sig = inspect.signature(f)
            bound_args = sig.bind(*args, **kwargs)
            if sys.version_info < (3, 10):
                # print('[white]Info: Python version < 3.10, skip type check[/white]')
                return f(*args, **kwargs)
            for name, value in bound_args.arguments.items():
                if name == 'self':
                    continue
                if name in sig.parameters:
                    if not isinstance(value, sig.parameters[name].annotation):
                        if sig.parameters[name].annotation is inspect._empty:
                            print(f'[white]Info: Argument {name} has no type annotation, skip type check[/white]')
                            continue
                        if self.raise_exception:
                            raise TypeError(f'Argument {name} must be of type {sig.parameters[name].annotation}')
                        else:
                            print(f'[red]Warning: Argument {name} must be of type {sig.parameters[name].annotation}[/red]')
            return f(*args, **kwargs)
        return wrapper