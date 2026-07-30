"""Shared helpers for example 17 (identity funnel)."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from graflo.connections import GraFloBackendConfig

EXAMPLE_DIR = Path(__file__).resolve().parent
DEFAULT_CSV_BACKEND_DIR = EXAMPLE_DIR / "artifacts" / "csv-backend"
MANIFEST_PATH = EXAMPLE_DIR / "manifest.yaml"


def backend_config(output_dir: str | Path) -> GraFloBackendConfig:
    return GraFloBackendConfig(output_dir=Path(output_dir))


@contextmanager
def example_workdir() -> Iterator[Path]:
    """Manifest file connectors use ``sub_path: data`` relative to this example."""
    previous_cwd = os.getcwd()
    try:
        os.chdir(EXAMPLE_DIR)
        yield EXAMPLE_DIR
    finally:
        os.chdir(previous_cwd)
