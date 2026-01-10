from typing import ContextManager
from planner import Recipe, Asset, DataAsset, Planner, inject, StaticRecipe
from tempfile import TemporaryDirectory
from dataclasses import dataclass
from contextlib import contextmanager, ExitStack
import logging
from pathlib import Path
logging.basicConfig(level=logging.DEBUG)


from planner.asset import RecipeSettings, RecipeContext




@dataclass
class StorageProviderAsset(Asset):
    """Provides both persistent and temporary storage to recipes."""
    root: Path
    project: str
    exitstack: ExitStack

    def get_temp(self) -> Path:
        dir = self.exitstack.enter_context(TemporaryDirectory())
        return Path(dir)

    def get_persistent(self) -> Path:
        raise NotImplementedError

    def _get_persistent(self, context: RecipeContext) -> Path:
        p = self.root
        if not context.settings.shared:
            p /= self.project
        p /= context.name
        return p


@dataclass
class StorageConfAsset(Asset):
    """Configuration for `StorageProviderAsset`."""
    root: Path | str | None = None
    project: str | None = None


class StorageProviderRecipe(Recipe):
    _makes = StorageProviderAsset
    _settings = RecipeSettings()

    conf: StorageConfAsset = inject()

    @contextmanager
    def make(self):
        exitstack = ExitStack()
        try:
            yield StorageProviderAsset(
                root=Path(self.conf.root) if self.conf.root else Path.cwd(),
                project=self.conf.project or "foo",
                exitstack=exitstack
            )
        finally:
            print("Closing storage provider exitstack")
            exitstack.close()





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
    _shared = True

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
        StorageConfAsset()
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