from typing import ContextManager
from planner import Recipe, Asset, DataAsset, Planner, inject
import logging
logging.basicConfig(level=logging.DEBUG)


class B_Asset(DataAsset[str]):
    def foo(self):
        return self.d
    
    def _foo(self, recipe: type[Recipe]):
        return f"{self.d}_{recipe.name}"


class B_Recipe(Recipe):
    _makes = B_Asset

    def make(self):
        return B_Asset("dummy_string")



class A_Asset(DataAsset[int]):
    pass


class A_Recipe(Recipe):
    _makes = A_Asset

    B: B_Asset = inject()

    def make(self):
        print()
        print(B_Asset.name)
        print(self.B.name)
        print(self.B.foo())
        # print()
        # print(self.B)
        # print()
        # print(self.B.__class__)
        print()
        return A_Asset(42)

    

plan = (
    Planner()
    .add(A_Recipe)
    .add(B_Recipe)
    .plan(A_Asset)
)

with plan.run() as res:
    print(res)
