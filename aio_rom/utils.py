from functools import singledispatch as ft_singledispatch
from functools import wraps


def type_dispatch(f):
    f = ft_singledispatch(f)

    @wraps(f)
    def inner(*args, **kw):
        if not args:
            raise TypeError(f"{f.__name__} requires at least 1 positional argument")
        return f.dispatch(args[0])(*args, **kw)

    def delegate(attr):
        return getattr(f, attr)

    inner.__getattribute__ = delegate
    return inner
