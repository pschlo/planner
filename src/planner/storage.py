from planner import Recipe, Asset, inject, Caps
from planner.caps import Cap, ContextCap
from tempfile import TemporaryDirectory
from dataclasses import dataclass
from contextlib import contextmanager, ExitStack
from pathlib import Path


@dataclass(frozen=True)
class StorageCap(Cap):
    tag: str | None = None

    shared: bool = False
    """
    Whether the persistent storage should be:
    - shared between projects (`True`), or
    - project-specific (`False`; default).
    """


@dataclass
class StorageProviderAsset(Asset):
    """Provides both persistent and temporary storage to recipes."""
    root: Path
    project: str | None
    exitstack: ExitStack

    def get_temp(self) -> Path:
        dir = self.exitstack.enter_context(TemporaryDirectory())
        return Path(dir)

    def get_persistent(self, *, caps = Caps()) -> Path:
        name, tag, shared = caps[ContextCap].name, caps[StorageCap].tag, caps[StorageCap].shared
        tag = tag or name.lower()

        # Resolve path
        if shared:
            path = (self.root / 'shared' / tag).resolve()
        else:
            if self.project is None:
                raise ValueError(f"Recipe workdir is project-specific, but Plan has no project set")
            path = (self.root / 'projects' / self.project / tag).resolve()

        # Validity check
        if not path.is_relative_to(self.root):
            raise ValueError(f"Recipe workdir path '{path}' escapes root")

        # Allow creation of missing relative path components
        path.mkdir(exist_ok=True, parents=True)

        return path


@dataclass
class StorageConfAsset(Asset):
    """Configuration for `StorageProviderAsset`."""
    root: Path | str
    project: str | None = None


class StorageProviderRecipe(Recipe):
    _makes = StorageProviderAsset

    conf: StorageConfAsset = inject()

    @contextmanager
    def make(self):
        exitstack = ExitStack()
        try:
            yield StorageProviderAsset(
                root=Path(self.conf.root).resolve(),
                project=self.conf.project or None,
                exitstack=exitstack
            )
        finally:
            exitstack.close()
