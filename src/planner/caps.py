from __future__ import annotations
from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass, field, Field
from typing import Self, TypeVar, ClassVar, Any, cast, dataclass_transform, Callable, TypeGuard, ContextManager, TypedDict, NotRequired, overload
from collections.abc import Collection, Mapping, Generator, Iterable, Hashable
from pathlib import Path
from functools import wraps
import itertools as it


class _CapMeta(type):
    def __repr__(cls):
        return cls.__name__


@dataclass(frozen=True)
class Cap(metaclass=_CapMeta):
    """A capability/setting of a recipe. May be read by asset methods."""


class Caps(Mapping[type[Cap], Cap]):
    """Capability container."""
    _data: dict[type[Cap], Cap]

    @overload
    def __init__(self) -> None: ...
    @overload
    def __init__(self, *caps: Iterable[Cap]) -> None: ...
    @overload
    def __init__(self, *caps: Cap) -> None: ...
    def __init__(self, *caps_or_iterables: Cap | Iterable[Cap]):
        self._data = dict()

        if not caps_or_iterables:
            return

        # If called as Caps(iterable), accept that.
        if not isinstance(caps_or_iterables[0], Cap):
            caps_or_iterables = cast(tuple[Iterable[Cap]], caps_or_iterables)
            _caps: Iterable[Cap] = it.chain(*caps_or_iterables)
        else:
            _caps = cast(tuple[Cap, ...], caps_or_iterables)

        for cap in _caps:
            self._data[type(cap)] = cap

    def __getitem__[T: Cap](self, key: type[T]) -> T:
        if key not in self._data:
            raise KeyError(f"Missing capability '{key}'")
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    @overload
    def get[T: Cap](self, key: type[T], default: None = None) -> T | None: ...
    @overload
    def get[T: Cap, D](self, key: type[T], default: D) -> T | D: ...
    def get[T: Cap, D](self, key: type[T], default: D | None = None) -> T | D | None: # type: ignore
        return cast(T, self._data.get(key, default))


@dataclass(frozen=True)
class ContextCap(Cap):
    recipe_name: str
    cache: dict[Hashable, Any] = field(default_factory=dict)

    def cached[T](self, key: object, factory: Callable[[], T]) -> T:
        if key in self.cache:
            print("Cache hit:", key)
            return self.cache[key]  # type: ignore[return-value]
        val = factory()
        self.cache[key] = val
        return val
