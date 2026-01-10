from __future__ import annotations
from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass, field, Field
from typing import Self, TypeVar, ClassVar, Any, cast, dataclass_transform, Callable, TypeGuard, ContextManager, TypedDict, NotRequired, overload
from collections.abc import Collection, Mapping, Generator, Iterable
from pathlib import Path
from functools import wraps
import itertools as it
import inspect


class _CapMeta(type):
    def __repr__(cls):
        return cls.__name__


@dataclass(frozen=True)
class Cap(metaclass=_CapMeta):
    """A capability/setting of a recipe. May be read by asset methods."""


class Caps:
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
            raise ValueError(f"Missing capability '{key}'")
        return self._data[key]
    
    def values(self):
        return self._data.values()


@dataclass(frozen=True)
class ContextCap(Cap):
    name: str


def inject(key: str | None = None):
    """
    Mark a field on a Recipe as a dependency to be injected by the repository.

    Usage on a Recipe:
    ```
    class MyRecipe(Recipe[FooAsset]):
        bar: BarAsset = inject()            # inject BarAsset with key="default"
        baz: BazAsset = inject("special")   # inject BazAsset with key="special"
    ```

    The repository finds marked fields and wires them up before calling `make()`.
    """
    metadata = dict(
        _injected=True,
        key=key,
    )
    return field(metadata=metadata)


def is_asset_class(obj) -> TypeGuard[type[Asset]]:
    return inspect.isclass(obj) and issubclass(obj, Asset)

@dataclass_transform(
    kw_only_default=True,
    frozen_default=True,
    field_specifiers=(field, Field)
)
class _Dataclass:
    """Any subclass will automatically become a dataclass (frozen, kw_only)."""

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        dataclass(
            cls,
            frozen=True,
            kw_only=True,
            **kwargs,
        )


class _RecipeMeta(ABCMeta, type):
    def __repr__(cls):
        return cls.__name__

    @property
    def name(cls):
        return cls.__name__


class Recipe[T: Asset](_Dataclass, ABC, metaclass=_RecipeMeta):
    """
    A Recipe *builds* an Asset.

    Contract:
    - `_makes` must be set to the Asset type this recipe produces.
    - `_dir` optionally declares a *persistent* working directory root;
      if left as `None`, the repository provides a temporary directory.
    - `workdir` is always a resolved absolute Path (either temp or persistent).
    - `make()` performs the build. It may:
        * return the Asset directly, or
        * be a generator that `yield`s the Asset once, then runs cleanup code
          after the caller consumes it (handy for resource teardown).

    Example generator pattern:
    ```
    def make(self):
        # produce
        asset = FooAsset(...)
        try:
            yield asset      # hand it to the repository
        finally:
            ...              # cleanup after the asset is released
    ```
    """
    def __init_subclass__(cls, **kwargs: Any):
        # Always inject ContextCap
        cls._caps = [ContextCap(cls.name), *cls._caps]
        return super().__init_subclass__(**kwargs)

    _makes: ClassVar[type[Asset]]  # override
    """Asset type produced by this Recipe (used by the repository/DI)."""

    _caps: ClassVar[Collection[Cap]]  = []  # override
    """The capabilities/settings of the recipe. These may be read by assets."""

    @abstractmethod
    def make(self) -> T | ContextManager[T]:
        """Build the asset. See class docstring for return/generator semantics."""
        ...


class RecipeBundle:
    """Collection of recipes, optionally together with their respective keys under which they provide their assets."""

    recipes: set[
        tuple[type[Recipe], str | None]
    ]
    """(Recipe, key) tuples"""

    def __init__(self, recipes: Collection[type[Recipe] | tuple[type[Recipe], str]]) -> None:
        self.recipes = {
            r if isinstance(r, tuple) else (r, None)
            for r in recipes
        }

    def __repr__(self) -> str:
        return f"RecipeBundle[{', '.join(map(repr, self.recipes))}]"


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


def StaticRecipe[T: Asset](asset: T) -> type[Recipe[T]]:
    """
    Create a trivial Recipe class that always returns the given `asset`.

    Useful for testing or for pinning a precomputed asset into the repository.

    Example:
    ```
    turbines = FullTurbinesAsset(ds)
    TurbinesStatic = StaticRecipe(turbines)
    repo.add(TurbinesStatic)  # registers a recipe that just returns `turbines`
    ```
    """
    
    asset_type = type(asset)
    class _StaticRecipe(Recipe[T]):
        _makes = asset_type
        _asset = asset
        
        def make(self):
            return self._asset
    
    # Optional: Better name for debugging
    _StaticRecipe.__name__ = f"StaticRecipe[{asset_type.__name__}]"
    
    return _StaticRecipe
