"""Pytest configuration for datapact tests.

This ensures tests run only on supported Python versions.
"""

from __future__ import annotations

import os
import sys
import pytest

os.environ.setdefault("DATAPACT_SKIP_EXEC_SUMMARY_BOOTSTRAP", "1")


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
