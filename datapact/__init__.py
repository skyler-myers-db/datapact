"""datapact package initialization."""

from __future__ import annotations

from importlib import metadata

try:
    __version__ = metadata.version("datapact")
except metadata.PackageNotFoundError:
    # Package isn't installed (e.g., running from a source checkout)
    __version__ = "0.0.0"

# Public package API could be exported here in the future.
