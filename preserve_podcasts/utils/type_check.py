from inspect import signature
from rich import print


class runtimeTypeCheck:
    """ Decorator to check types at runtime. """
    def __init__(self, raise_exception=True):
        self.raise_exception = raise_exception

    def __call__(self, f):
        def wrapper(*args, **kwargs):
            sig = signature(f)
            bound_args = sig.bind(*args, **kwargs)
            for name, value in bound_args.arguments.items():
                if name in sig.parameters:
                    if not isinstance(value, sig.parameters[name].annotation):
                        if self.raise_exception:
                            raise TypeError(f'Argument {name} must be of type {sig.parameters[name].annotation}')
                        else:
                            print(f'[red]Warning: Argument {name} must be of type {sig.parameters[name].annotation}[/red]')
            return f(*args, **kwargs)
        return wrapper