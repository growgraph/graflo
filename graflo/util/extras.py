"""Optional dependencies, imported only when a feature that needs them runs.

Every database driver and source integration lives behind a ``graflo`` extra
(``graflo[neo4j]``, ``graflo[kafka]``, ``graflo[all]``…). Modules that use one
fetch it through :func:`require`, never at import time, so importing graflo,
its contract or its capability tables loads no driver.
"""

from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Any

_HINT = (
    "{feature} needs the `{extra}` extra, which is not installed for {python} "
    "(missing module {module!r}): install it with `uv add 'graflo[{extra}]'` "
    "(or `pip install 'graflo[{extra}]'`)"
)


class MissingExtraError(ImportError):
    """An optional dependency is not installed; the message names the extra."""

    def __init__(self, extra: str, module: str, feature: str | None = None) -> None:
        self.extra = extra
        super().__init__(
            _HINT.format(
                feature=feature or "This feature",
                extra=extra,
                python=sys.executable,
                module=module,
            ),
            name=module,
        )


def require(module: str, extra: str, *, feature: str | None = None) -> ModuleType:
    """Import *module*, or raise naming the extra that provides it.

    Args:
        module: Dotted module name (``"neo4j"``, ``"arango.exceptions"``).
        extra: The graflo extra that installs it.
        feature: What needs it, for the message.

    Raises:
        MissingExtraError: The module, or one it imports, is not installed.
    """
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as exc:
        raise MissingExtraError(extra, exc.name or module, feature) from exc


class _LazyModule(ModuleType):
    """Stands in for a module until an attribute is read, then imports it.

    Attributes are read through on every access (only the module is kept), so
    patching the real module, as tests do, is seen here.
    """

    def __init__(self, module: str, extra: str) -> None:
        super().__init__(module)
        self.__dict__["_graflo_extra"] = extra
        self.__dict__["_graflo_module"] = None

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        real = self.__dict__["_graflo_module"]
        if real is None:
            real = require(self.__name__, self.__dict__["_graflo_extra"])
            self.__dict__["_graflo_module"] = real
        return getattr(real, name)


def lazy_module(module: str, extra: str) -> ModuleType:
    """A module that is imported on first attribute access.

    Use it at module level for a driver, with the real import under
    ``TYPE_CHECKING`` so the type checker sees the driver's types::

        if TYPE_CHECKING:
            import neo4j
        else:
            neo4j = lazy_module("neo4j", "neo4j")

    Raises:
        MissingExtraError: On first attribute access, if *extra* is missing.
    """
    return _LazyModule(module, extra)
