from __future__ import annotations
from dataclasses import field
from typing import TypeGuard
import inspect

from .asset import Asset


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
