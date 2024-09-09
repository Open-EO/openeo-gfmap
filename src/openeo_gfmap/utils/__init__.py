"""This sub-module contains utilitary function and tools for OpenEO-GFMap"""

import logging

from openeo_gfmap.utils.build_df import load_json
from openeo_gfmap.utils.intervals import quintad_intervals
from openeo_gfmap.utils.netcdf import update_nc_attributes
from openeo_gfmap.utils.split_stac import split_collection_by_epsg
from openeo_gfmap.utils.tile_processing import (
    array_bounds,
    arrays_cosine_similarity,
    normalize_array,
    select_optical_bands,
    select_sar_bands,
)

_log = logging.getLogger(__name__)
_log.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

_log.addHandler(ch)


__all__ = [
    "load_json",
    "normalize_array",
    "select_optical_bands",
    "array_bounds",
    "select_sar_bands",
    "arrays_cosine_similarity",
    "quintad_intervals",
    "split_collection_by_epsg",
    "update_nc_attributes",
]
