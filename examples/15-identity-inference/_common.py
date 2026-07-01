"""Shared helpers for example 15 (identity inference)."""

from __future__ import annotations

import csv
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from graflo.db.graflo_backend.config import GraFloBackendConfig
from graflo.db.identity_inference import IdentityInferenceConfig

EXAMPLE_DIR = Path(__file__).resolve().parent
DEFAULT_INFERRED_MANIFEST = EXAMPLE_DIR / "artifacts" / "manifest-inferred.yaml"
DEFAULT_CSV_BACKEND_DIR = EXAMPLE_DIR / "artifacts" / "csv-backend"

VERTEX_CSV_MAP: dict[str, str] = {
    "product": "products.csv",
    "supplier": "suppliers.csv",
}

DEFAULT_INFERENCE_CONFIG = IdentityInferenceConfig(min_sample_size=100)


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


def load_vertex_samples(vertex_name: str) -> list[dict[str, object]]:
    """Load CSV rows for a vertex type as plain dict samples."""
    csv_name = VERTEX_CSV_MAP[vertex_name]
    path = EXAMPLE_DIR / "data" / csv_name
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
