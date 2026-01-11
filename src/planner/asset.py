from __future__ import annotations
from abc import ABC, ABCMeta
from dataclasses import dataclass, field
from typing import Self, Any, cast, TYPE_CHECKING
from collections.abc import Hashable
from pathlib import Path
from functools import wraps
import inspect

from .caps import Caps, ContextCap
if TYPE_CHECKING:
    from .recipe import Recipe


class _AssetMeta(ABCMeta, type):
    def __repr__(cls):
        return cls.__name__

    @property
    def name(cls):
        return cls.__name__


class Asset(ABC, metaclass=_AssetMeta):
    """Marker base class for all assets produced/consumed by Recipes."""

    def _for_recipe(self, recipe_context: Recipe) -> Self:
        return cast(Self, _BoundAsset(self, recipe_context))


class _BoundAsset[T: Asset]:
    """A bound façade: exposes the same public methods as T,
    but injects a `ContextCap` and Recipe-defined caps into methods that expect a `caps` parameter."""
    _target: T
    _recipe_context: Recipe
    _context_cap: ContextCap
    _wrapper_cache: dict[Hashable, Any]

    def __init__(self, target: T, recipe_context: Recipe) -> None:
        self._target = target
        self._recipe_context = recipe_context
        self._context_cap = ContextCap(
            recipe_name=type(recipe_context).name
        )
        self._wrapper_cache = dict()

    def __getattr__(self, name: str) -> Any:
        if name in self._wrapper_cache:
            return self._wrapper_cache[name]

        attr = getattr(self._target, name)
        if not callable(attr):
            return attr

        # Decide whether to inject caps based on signature
        try:
            sig = inspect.signature(attr)
        except (TypeError, ValueError):
            # builtins / C-extensions may not have signatures
            return attr

        if "caps" not in sig.parameters:
            return attr

        # Wrap: inject caps, but override with provided caps
        @wraps(attr)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if 'caps' in kwargs:
                assert isinstance(c := kwargs['caps'], Caps)
                passed_caps = list(c.values())
            else:
                passed_caps = []
            kwargs['caps'] = Caps([self._context_cap, *self._recipe_context._caps, *passed_caps])
            return attr(*args, **kwargs)

        self._wrapper_cache[name] = wrapped
        return wrapped


@dataclass(frozen=True)
class DataAsset[T](Asset):
    """Simple wrapper around a data payload."""
    data: T

    @property
    def d(self):
        """Shorthand accessor for the wrapped data."""
        return self.data
