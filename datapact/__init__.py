"""datapact package initialization.

Enforces a minimum Python version at import time to ensure compatibility
with pinned dependencies and language features used in this project.
"""

from __future__ import annotations

import sys


MIN_PY = (3, 13, 5)

if sys.version_info < MIN_PY:
    ver = ".".join(map(str, MIN_PY))
    raise RuntimeError(
        f"datapact requires Python {ver}+; found {sys.version.split()[0]}"
    )

# Public package API could be exported here in the future.
