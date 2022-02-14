from functools import singledispatch as ft_singledispatch
from functools import wraps


def type_dispatch(f):
    f = ft_singledispatch(f)

    @wraps(f)
    async def inner(value_type: type, *args, **kw):
        if not value_type:
            raise TypeError(f"{f.__name__} requires at least 1 positional argument")

        value = await f.dispatch(value_type)(value_type, *args, **kw)
        if not isinstance(value, value_type):
            raise TypeError(f"{value} is not an instance of {value_type}")
        return value

    def delegate(attr):
        return getattr(f, attr)

    inner.__getattribute__ = delegate
    return inner
