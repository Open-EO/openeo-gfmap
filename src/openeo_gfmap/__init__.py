"""OpenEO General Framework for Mapping.

Simplify the development of mapping applications through Remote Sensing data
by leveraging the power of OpenEO (http://openeo.org/).

More information available in the README.md file.
"""

from .backend import _BackendType
from .fetching import FetchType
from .metadata import FakeMetadata
from .spatial import BoundingBoxExtent, SpatialContext
from .temporal import TemporalContext

__all__ = [
    "_BackendType",
    "SpatialContext",
    "BoundingBoxExtent",
    "TemporalContext",
    "FakeMetadata",
    "FetchType",
]
