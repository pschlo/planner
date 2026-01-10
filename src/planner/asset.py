from __future__ import annotations
from abc import ABC, ABCMeta
from dataclasses import dataclass
from typing import Self, Any, cast, TYPE_CHECKING
from pathlib import Path
from functools import wraps
import inspect

from .caps import Caps
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

    def _for_recipe(self, recipe: type[Recipe]) -> Self:
        return cast(Self, _BoundAsset(self, recipe))


@dataclass(frozen=True)
class _BoundAsset[T: Asset]:
    """A bound façade: exposes the same public methods as T,
    but injects recipe into underlying _method implementations."""
    _target: T
    _recipe: type[Recipe]

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)

        if not callable(attr):
            return attr

        # Decide whether to inject ctx based on signature
        try:
            sig = inspect.signature(attr)
        except (TypeError, ValueError):
            # builtins / C-extensions may not have signatures
            return attr

        if "caps" not in sig.parameters:
            return attr

        # Wrap: inject ctx unless caller explicitly provided it
        @wraps(attr)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if 'caps' in kwargs:
                assert isinstance(c := kwargs['caps'], Caps)
                passed_caps = list(c.values())
            else:
                passed_caps = []
            kwargs['caps'] = Caps(self._recipe._caps, passed_caps)
            return attr(*args, **kwargs)
        return wrapped


@dataclass(frozen=True)
class DataAsset[T](Asset):
    """Simple wrapper around a data payload."""
    data: T

    @property
    def d(self):
        """Shorthand accessor for the wrapped data."""
        return self.data
