"""OpenEO General Framework for Mapping.

Simplify the development of mapping applications through Remote Sensing data
by levearging the power of OpenEO (http://openeo.org/).

More information available in the README.md file.
"""

from ._version import __version__
from .backend_context import Backend, BackendContext

__all__ = ["__version__", "Backend", "BackendContext"]
