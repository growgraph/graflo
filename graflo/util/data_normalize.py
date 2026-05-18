"""Normalize tabular input into document rows."""

from __future__ import annotations

from typing import cast

import pandas as pd


def normalize_rows(
    data: pd.DataFrame | list[list] | list[dict], columns: list[str] | None = None
) -> list[dict]:
    """Normalize resource data into a list of dictionaries."""
    if isinstance(data, pd.DataFrame):
        columns = data.columns.tolist()
        _data = data.values.tolist()
    elif data and isinstance(data[0], list):
        _data = cast(list[list], data)
        if columns is None:
            raise ValueError("columns should be set")
    else:
        return cast(list[dict], data)
    return [{k: v for k, v in zip(columns, item)} for item in _data]
