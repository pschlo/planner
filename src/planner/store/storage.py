from planner import Recipe, Asset, inject, Caps
from planner.caps import Cap, ContextCap
from tempfile import TemporaryDirectory
from dataclasses import dataclass, asdict, astuple
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
    _root: Path
    _project: str | None
    _exitstack: ExitStack
    

    def tempdir(self) -> Path:
        dir = self._exitstack.enter_context(TemporaryDirectory())
        return Path(dir)

    def persistent_dir(self, *, caps = Caps()) -> Path:
        # Read capabilities
        _cap = caps.get(ContextCap)
        _name = _cap.name if _cap else None

        _cap = caps.get(StorageCap, StorageCap())
        _tag, _shared = _cap.tag, _cap.shared

        # Determine values
        tag = None
        if _tag is not None:
            tag = _tag
        elif _name is not None:
            tag = _name.lower()
        else:
            raise ValueError("Missing storage name")

        shared = _shared

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
