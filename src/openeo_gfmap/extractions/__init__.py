"""Extraction sub-module.

Logic behind the extraction of training or inference data. Different backends
are supported in order to obtain a very similar result at the end of this
component.
"""

from .extraction import CollectionExtractor
from .s2 import build_sentinel2_l2a_extractor

__all__ = ["build_sentinel2_l2a_extractor", "CollectionExtractor"]
