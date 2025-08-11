"""Pytest configuration for datapact tests.

This ensures tests run only on supported Python versions.
"""
from __future__ import annotations

import sys
import pytest


MIN_PY = (3, 13, 5)


def pytest_collection_modifyitems(config, items):
    # silence unused argument warning while keeping correct hook signature
    _ = config
    if sys.version_info >= MIN_PY:
        return
    ver = ".".join(map(str, MIN_PY))
    skip_mark = pytest.mark.skip(reason=f"Tests require Python {ver}+.")
    for item in items:
        item.add_marker(skip_mark)
