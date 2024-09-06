"""Extraction sub-module.

Logic behind the extraction of training or inference data. Different backends
are supported in order to obtain a very similar result at the end of this
component.
"""

import logging

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

_log.addHandler(ch)

__all__ = [
    "build_sentinel2_l2a_extractor",
    "CollectionFetcher",
    "FetchType",
    "build_sentinel1_grd_extractor",
    "build_generic_extractor",
    "build_generic_extractor_stac",
]

from .fetching import CollectionFetcher, FetchType  # noqa: E402
from .generic import build_generic_extractor, build_generic_extractor_stac  # noqa: E402
from .s1 import build_sentinel1_grd_extractor  # noqa: E402
from .s2 import build_sentinel2_l2a_extractor  # noqa: E402
