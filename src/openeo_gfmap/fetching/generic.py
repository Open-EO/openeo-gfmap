""" Generic extraction of features, supporting VITO backend.
"""

from typing import Callable, Optional

import openeo
from openeo.rest import OpenEoApiError

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.fetching import CollectionFetcher, FetchType, _log
from openeo_gfmap.fetching.commons import (
    _load_collection,
    convert_band_names,
    rename_bands,
    resample_reproject,
)
from openeo_gfmap.spatial import SpatialContext
from openeo_gfmap.temporal import TemporalContext

BASE_DEM_MAPPING = {"DEM": "COP-DEM"}
BASE_WEATHER_MAPPING = {
    "dewpoint-temperature": "AGERA5-DEWTEMP",
    "precipitation-flux": "AGERA5-PRECIP",
    "solar-radiation-flux": "AGERA5-SOLRAD",
    "temperature-max": "AGERA5-TMAX",
    "temperature-mean": "AGERA5-TMEAN",
    "temperature-min": "AGERA5-TMIN",
    "vapour-pressure": "AGERA5-VAPOUR",
    "wind-speed": "AGERA5-WIND",
}
KNOWN_UNTEMPORAL_COLLECTIONS = ["COPERNICUS_30"]


def _get_generic_fetcher(
    collection_name: str, fetch_type: FetchType, backend: Backend
) -> Callable:
    band_mapping: Optional[dict] = None

    if collection_name == "COPERNICUS_30":
        band_mapping = BASE_DEM_MAPPING
    elif collection_name == "AGERA5":
        band_mapping = BASE_WEATHER_MAPPING

    def generic_default_fetcher(
        connection: openeo.Connection,
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        bands: list,
        **params,
    ) -> openeo.DataCube:
        if band_mapping is not None:
            bands = convert_band_names(bands, band_mapping)

        if (collection_name in KNOWN_UNTEMPORAL_COLLECTIONS) and (
            temporal_extent is not None
        ):
            _log.warning(
                "User set-up non None temporal extent for %s collection. Ignoring it.",
                collection_name,
            )
            temporal_extent = None

        try:
            cube = _load_collection(
                connection,
                bands,
                collection_name,
                spatial_extent,
                temporal_extent,
                fetch_type,
                **params,
            )
        except OpenEoApiError as e:
            if "CollectionNotFound" in str(e):
                raise ValueError(
                    f"Collection {collection_name} not found in the selected backend {backend.value}."
                ) from e
            raise e

        # # Apply if the collection is a GeoJSON Feature collection
        # if isinstance(spatial_extent, GeoJSON):
        #     cube = cube.filter_spatial(spatial_extent)

        return cube

    return generic_default_fetcher


def _get_generic_processor(collection_name: str, fetch_type: FetchType) -> Callable:
    """Builds the preprocessing function from the collection name as it stored
    in the target backend.
    """
    band_mapping: Optional[dict] = None
    if collection_name == "COPERNICUS_30":
        band_mapping = BASE_DEM_MAPPING
    elif collection_name == "AGERA5":
        band_mapping = BASE_WEATHER_MAPPING

    def generic_default_processor(cube: openeo.DataCube, **params):
        """Default collection preprocessing method for generic datasets.
        This method renames bands and removes the time dimension in case the
        requested dataset is DEM
        """
        if params.get("target_resolution", None) is not None:
            cube = resample_reproject(
                cube,
                params.get("target_resolution", 10.0),
                params.get("target_crs", None),
                method=params.get("resampling_method", "near"),
            )

        if collection_name == "COPERNICUS_30":
            cube = cube.min_time()

        if band_mapping is not None:
            cube = rename_bands(cube, band_mapping)

        return cube

    return generic_default_processor


def build_generic_extractor(
    backend_context: BackendContext,
    bands: list,
    fetch_type: FetchType,
    collection_name: str,
    **params,
) -> CollectionFetcher:
    """Creates a generic extractor adapted to the given backend. Currently only tested with VITO backend"""
    fetcher = _get_generic_fetcher(collection_name, fetch_type, backend_context.backend)
    preprocessor = _get_generic_processor(collection_name, fetch_type)

    return CollectionFetcher(backend_context, bands, fetcher, preprocessor, **params)
