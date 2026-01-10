from planner import Recipe, Asset, DataAsset, Planner, inject, StaticRecipe, Caps, StorageProviderRecipe, StorageProviderAsset, StorageConfAsset, StorageCap
import logging
from pathlib import Path
logging.basicConfig(level=logging.DEBUG)


class B_Asset(DataAsset[str]):
    def foo(self):
        return self.d
    
    def _foo(self, recipe: type[Recipe]):
        return f"{self.d}_{recipe.name}"


class B_Recipe(Recipe):
    _makes = B_Asset

    storage: StorageProviderAsset = inject()

    def make(self):
        p = self.storage.get_temp()
        print(str(p))
        return B_Asset("dummy_string")


class A_Asset(DataAsset[int]):
    pass


class A_Recipe(Recipe):
    _makes = A_Asset
    _caps = [
        StorageCap(
            tag="a_recipe",
            shared=True,
        )
    ]

    B: B_Asset = inject()
    storage: StorageProviderAsset = inject()

    def make(self):
        print(self.storage.get_persistent())
        return A_Asset(42)


plan = (
    Planner()
    .add(A_Recipe)
    .add(B_Recipe)
    .add(StorageProviderRecipe)
    .add(StaticRecipe(
        StorageConfAsset(
            root="debug/resources",
            project="foo"
        )
    ))
    .plan(A_Asset)
)

with plan.run(defer_cleanup=True) as res:
    print(res)
    import time
    time.sleep(2)

print("done")
import time
time.sleep(2)