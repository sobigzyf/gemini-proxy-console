"""
Lightweight distutils compatibility shim.

Provides only what this repository needs (`distutils.version.LooseVersion`).
"""

from .version import LooseVersion  # noqa: F401

