"""ehio — bridge between Airtable and Drakkar workflows."""

from __future__ import annotations
from importlib.metadata import version, PackageNotFoundError

__all__ = ["__version__"]

try:
    __version__ = version("ehio")
except PackageNotFoundError:
    __version__ = "unknown"
