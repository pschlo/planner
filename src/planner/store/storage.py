from planner import Recipe, Asset, inject, Caps
from planner.caps import Cap, ContextCap
from tempfile import TemporaryDirectory
from dataclasses import dataclass, asdict, astuple
from contextlib import contextmanager, ExitStack
from pathlib import Path
from enum import Enum, auto


@dataclass(frozen=True)
class StorageCap(Cap):
    tag: str | None = None

    shared: bool = False
    """
    Whether the persistent storage should be:
    - shared between projects (`True`), or
    - project-specific (`False`; default).
    """


class CacheKey(Enum):
    TEMPDIR = auto()
    PERSISTENT_DIR = auto()

@dataclass
class StorageProviderAsset(Asset):
    """Provides both persistent and temporary storage to recipes."""
    _root: Path
    _project: str | None
    _exitstack: ExitStack

    def tempdir(self, *, caps=Caps()) -> Path:
        ctx = caps.get(ContextCap)
        f = self._create_tempdir
        return f() if not ctx else ctx.cached(CacheKey.TEMPDIR, f)

    def _create_tempdir(self) -> Path:
        dir = self._exitstack.enter_context(TemporaryDirectory())
        return Path(dir)

    def persistent_dir(self, *, caps=Caps()) -> Path:
        # Read capabilities
        ctx = caps.get(ContextCap)
        name = ctx.recipe_name if ctx else None
        sc = caps.get(StorageCap, StorageCap())
        tag = sc.tag or (name.lower() if name else None)
        if tag is None:
            raise ValueError("Missing storage name")
        shared = sc.shared

        f = lambda: self._create_persistent_dir(tag=tag, shared=shared)
        return f() if not ctx else ctx.cached((CacheKey.PERSISTENT_DIR, tag, shared), f)

    def _create_persistent_dir(self, tag: str, shared: bool) -> Path:
        # Resolve path
        if shared:
            path = (self._root / 'shared' / tag).resolve()
        else:
            if self._project is None:
                raise ValueError(f"Recipe workdir is project-specific, but Plan has no project set")
            path = (self._root / 'projects' / self._project / tag).resolve()

        # Validity check
        if not path.is_relative_to(self._root):
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
                _root=Path(self.conf.root).resolve(),
                _project=self.conf.project or None,
                _exitstack=exitstack
            )
        finally:
            exitstack.close()
