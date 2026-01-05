from __future__ import annotations
from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass, field, Field
from typing import Self, TypeVar, ClassVar, Any, cast, dataclass_transform, Callable, TypeGuard, ContextManager
from collections.abc import Collection, Mapping, Generator
from pathlib import Path
from tempfile import TemporaryDirectory
from functools import wraps
import inspect


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


class RecipeMeta(ABCMeta, type):
    def __repr__(cls):
        return cls.__name__

    @property
    def name(cls):
        return cls.__name__


class Recipe[T: Asset](_Dataclass, ABC, metaclass=RecipeMeta):
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
    _makes: ClassVar[type[Asset]]  # override
    """Asset type produced by this Recipe (used by the repository/DI)."""

    _dir: ClassVar[Path | str | None] = None  # override
    """
    Persistent storage hint for this recipe.

    - None (default): repository assigns a *temporary* directory (auto-cleaned).
    - Relative Path/str: repository will resolve under its configured root.

    The repository may also append key/version subfolders to avoid collisions.
    """

    _shared: ClassVar[bool] = False  # override
    """
    Whether the persistent storage should be:
    - shared between projects (`True`), or
    - project-specific (`False`; default).
    """

    workdir: Path
    """Resolved *absolute* working directory for this build. Use this for any filesystem I/O."""

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


class AssetMeta(ABCMeta, type):
    def __repr__(cls):
        return cls.__name__

    @property
    def name(cls):
        return cls.__name__


class Asset(ABC, metaclass=AssetMeta):
    """Marker base class for all assets produced/consumed by Recipes."""

    def _for_recipe(self, recipe: type[Recipe]) -> Self:
        return cast(Self, BoundAsset(self, recipe))

    @property
    def name(self):
        return self.__class__.name


@dataclass(frozen=True)
class BoundAsset[T: Asset]:
    """A bound façade: exposes the same public methods as T,
    but injects recipe into underlying _method implementations."""
    _target: T
    _recipe: type[Recipe]

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._target, name)

        # Only wrap callables that have a contextful implementation
        core_name = f"_{name}"
        core = getattr(self._target, core_name, None)
        if callable(core):
            @wraps(attr)
            def wrapped(*args: Any, **kwargs: Any) -> Any:
                return core(self._recipe, *args, **kwargs)
            return wrapped

        # Fallback: normal attribute/method access
        return attr


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
