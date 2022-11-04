from __future__ import annotations

import operator
import typing

from aio_rom.types import IModel, Key

T = typing.TypeVar("T", bound=IModel)


class ProxyModel(IModel, typing.Generic[T]):
    __slots__ = ("proxied", "proxied_type", "id")

    def __init__(self, proxied_type: type[T], id: Key):
        self.id: Key = id
        self.proxied: T | None = None
        self.proxied_type: type[T] = proxied_type

    @property
    def __class__(self):
        return self.proxied_type

    @__class__.setter
    def __class__(self, value):
        self.proxied_type = value

    async def save(self, *, optimistic: bool = False, cascade: bool = False) -> None:
        if self.proxied:
            await self.proxied.save(optimistic=optimistic, cascade=cascade)

    @classmethod
    async def get(cls, *args):
        raise TypeError("Proxy objects can't be fetched")

    async def total_count(self) -> int:
        if self.proxied:
            return await self.proxied.total_count()
        raise TypeError("Proxy objects can't be counted")

    async def delete(self, cascade: bool = False) -> None:
        if self.proxied:
            return await self.proxied.delete(cascade=cascade)

    async def refresh(self) -> None:
        self.proxied = await self.proxied_type.get(self.id)

    def __iter__(self):
        return iter(self.proxied)

    def __getattr__(self, item):
        if not self.proxied:
            raise ValueError("You must call `refresh()` before you can use this proxy")
        return getattr(self.proxied, item)

    def __lt__(self, other):
        return self.proxied < other

    def __le__(self, other):
        return self.proxied <= other

    def __eq__(self, other):
        return self.proxied == other

    def __ne__(self, other):
        return self.proxied != other

    def __gt__(self, other):
        return self.proxied > other

    def __ge__(self, other):
        return self.proxied >= other

    def __hash__(self):
        return hash(self.proxied)

    def __nonzero__(self):
        return bool(self.proxied)

    def __bool__(self):
        return bool(self.proxied)

    def __add__(self, other):
        return self.proxied + other

    def __sub__(self, other):
        return self.proxied - other

    def __mul__(self, other):
        return self.proxied * other

    def __truediv__(self, other):
        return operator.truediv(self.proxied, other)

    def __floordiv__(self, other):
        return self.proxied // other

    def __mod__(self, other):
        return self.proxied % other

    def __divmod__(self, other):
        return divmod(self.proxied, other)

    def __lshift__(self, other):
        return self.proxied << other

    def __rshift__(self, other):
        return self.proxied >> other

    def __and__(self, other):
        return self.proxied & other

    def __xor__(self, other):
        return self.proxied ^ other

    def __or__(self, other):
        return self.proxied | other

    def __radd__(self, other):
        return other + self.proxied

    def __rsub__(self, other):
        return other - self.proxied

    def __rmul__(self, other):
        return other * self.proxied

    def __rtruediv__(self, other):
        return operator.truediv(other, self.proxied)

    def __rfloordiv__(self, other):
        return other // self.proxied

    def __rmod__(self, other):
        return other % self.proxied

    def __rdivmod__(self, other):
        return divmod(other, self.proxied)

    def __rpow__(self, other, *args):
        return pow(other, self.proxied, *args)

    def __rlshift__(self, other):
        return other << self.proxied

    def __rrshift__(self, other):
        return other >> self.proxied

    def __rand__(self, other):
        return other & self.proxied

    def __rxor__(self, other):
        return other ^ self.proxied

    def __ror__(self, other):
        return other | self.proxied

    def __iadd__(self, other):
        self.proxied += other
        return self

    def __isub__(self, other):
        self.proxied -= other
        return self

    def __imul__(self, other):
        self.proxied *= other
        return self

    def __itruediv__(self, other):
        self.proxied = operator.itruediv(self.proxied, other)
        return self

    def __ifloordiv__(self, other):
        self.proxied //= other
        return self

    def __imod__(self, other):
        self.proxied %= other
        return self

    def __ilshift__(self, other):
        self.proxied <<= other
        return self

    def __irshift__(self, other):
        self.proxied >>= other
        return self

    def __iand__(self, other):
        self.proxied &= other
        return self

    def __ixor__(self, other):
        self.proxied ^= other
        return self

    def __ior__(self, other):
        self.proxied |= other
        return self

    def __neg__(self):
        return -self.proxied

    def __pos__(self):
        return +self.proxied

    def __abs__(self):
        return abs(self.proxied)

    def __invert__(self):
        return ~self.proxied

    def __int__(self):
        return int(self.proxied)

    def __float__(self):
        return float(self.proxied)

    def __complex__(self):
        return complex(self.proxied)

    def __oct__(self):
        return oct(self.proxied)

    def __hex__(self):
        return hex(self.proxied)

    def __index__(self):
        return operator.index(self.proxied)

    def __len__(self):
        return len(self.proxied)

    def __contains__(self, value):
        return value in self.proxied

    def __getitem__(self, key):
        return self.proxied[key]

    def __setitem__(self, key, value):
        self.proxied[key] = value

    def __delitem__(self, key):
        del self.proxied[key]

    def __getslice__(self, i, j):
        return self.proxied[i:j]

    def __setslice__(self, i, j, value):
        self.proxied[i:j] = value

    def __delslice__(self, i, j):
        del self.proxied[i:j]

    def __enter__(self):
        return self.proxied.__enter__()

    def __exit__(self, *args, **kwargs):
        return self.proxied.__exit__(*args, **kwargs)

    def __copy__(self):
        raise NotImplementedError("object proxy must define __copy__()")

    def __deepcopy__(self, memo):
        raise NotImplementedError("object proxy must define __deepcopy__()")

    def __reduce__(self):
        raise NotImplementedError("object proxy must define __reduce_ex__()")

    def __reduce_ex__(self, protocol):
        raise NotImplementedError("object proxy must define __reduce_ex__()")
