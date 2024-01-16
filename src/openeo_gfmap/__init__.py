"""OpenEO General Framework for Mapping.

Simplify the development of mapping applications through Remote Sensing data
by leveraging the power of OpenEO (http://openeo.org/).

More information available in the README.md file.
"""

from .backend import Backend, BackendContext
from .metadata import FakeMetadata
from .spatial import SpatialContext, BoundingBoxExtent
from .temporal import TemporalContext
from .fetching import FetchType

__all__ = [
    "Backend",
    "BackendContext",
    "SpatialContext",
    "BoundingBoxExtent",
    "TemporalContext",
    "FakeMetadata",
    "FetchType"
]
