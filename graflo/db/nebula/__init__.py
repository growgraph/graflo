"""NebulaGraph database implementation.

Supports NebulaGraph 3.x (nGQL via nebula3-python) and 5.x (ISO GQL via nebula5-python).
"""

from .conn import NebulaConnection

__all__ = [
    "NebulaConnection",
]
