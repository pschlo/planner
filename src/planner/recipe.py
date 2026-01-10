from __future__ import annotations
from abc import ABC, abstractmethod, ABCMeta
from dataclasses import dataclass, field, Field
from typing import ClassVar, Any, cast, dataclass_transform, ContextManager, TYPE_CHECKING
from collections.abc import Collection

from .caps import Cap, ContextCap
from .asset import Asset


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
