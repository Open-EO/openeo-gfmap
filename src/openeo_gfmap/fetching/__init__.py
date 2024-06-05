"""Extraction sub-module.

Logic behind the extraction of training or inference data. Different backends
are supported in order to obtain a very similar result at the end of this
component.
"""

import logging

from .fetching import CollectionFetcher, FetchType
from .s1 import build_sentinel1_grd_extractor
from .s2 import build_sentinel2_l2a_extractor

_log = logging.getLogger(__name__)

__all__ = [
    "build_sentinel2_l2a_extractor",
    "CollectionFetcher",
    "FetchType",
    "build_sentinel1_grd_extractor",
]
