"""
Minimal `distutils.version` shim for Python 3.12+ environments.
"""

from __future__ import annotations

import re
from functools import total_ordering
from typing import Tuple


def _normalize(vstring: str) -> Tuple[tuple, ...]:
    tokens = re.findall(r"[0-9]+|[A-Za-z]+", str(vstring or ""))
    out = []
    for tok in tokens:
        if tok.isdigit():
            out.append((0, int(tok)))
        else:
            out.append((1, tok.lower()))
    return tuple(out)


@total_ordering
class LooseVersion:
    def __init__(self, vstring: str = "") -> None:
        self.vstring = str(vstring or "")
        self.version = _normalize(self.vstring)

    def __repr__(self) -> str:
        return f"LooseVersion('{self.vstring}')"

    def __str__(self) -> str:
        return self.vstring

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LooseVersion):
            other = LooseVersion(str(other))
        return self.version == other.version

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, LooseVersion):
            other = LooseVersion(str(other))
        return self.version < other.version

