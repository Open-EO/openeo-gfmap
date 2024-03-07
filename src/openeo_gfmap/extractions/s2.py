""" Extraction of S2 features, depending on the backend.
"""
from typing import Callable

import openeo
from geojson import GeoJSON

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.spatial import BoundingBoxExtent, SpatialContext
from openeo_gfmap.temporal import TemporalContext

from .commons import rename_bands, resample_reproject
from .extraction import CollectionExtractor

BASE_SENTINEL2_L2A_MAPPING = {
    "B01": "S2-B01",
    "B02": "S2-B02",
    "B03": "S2-B03",
    "B04": "S2-B04",
    "B05": "S2-B05",
    "B06": "S2-B06",
    "B07": "S2-B07",
    "B08": "S2-B08",
    "B8A": "S2-B8A",
    "B09": "S2-B09",
    "B11": "S2-B11",
    "B12": "S2-B12",
    "AOT": "S2-AOT",
    "SCL": "S2-SCL",
    "SNW": "S2-SNW",
    "CLD": "S2-CLD",
    "CLP": "s2cloudless-CLP",
    "CLM": "s2clodless-CLM",
}


BAND_MAPPINGS = {
    "SENTINEL2_L2A": BASE_SENTINEL2_L2A_MAPPING,
}


def get_s2_l2a_default_fetcher(collection_name: str) -> Callable:
    """Builds the fetch function from the collection name as it stored in the
    target backend.
    """

    def s2_l2a_fetch_default(
        connection: openeo.Connection,
        spatial_extent: SpatialContext,
        temporal_extent: TemporalContext,
        bands: list,
        **params,
    ) -> openeo.DataCube:
        """Default collection fetcher for Sentinel_L2A collections.
        Parameters
        ----------
        connection: openeo.Connection
            Connection to a general backend.
        spatial_extent: SpatialContext
            Either a GeoJSON collection or a bounding box of locations.
            Performs spatial filtering if the spatial context is a GeoJSON
            collection, as it implies sparse data.
        temporal_extent: TemporalContexct
            A time range, defined by a start and end date.
        bands: list
            The name of the bands to load from that collection
        Returns
        -------
        openeo.DataCube: a datacube containing the collection raw products.
        """
        if isinstance(spatial_extent, BoundingBoxExtent):
            spatial_extent = dict(spatial_extent)
        elif isinstance(spatial_extent, GeoJSON):
            assert (
                spatial_extent.get("crs", None) is not None
            ), "CRS not defined within GeoJSON collection."
            spatial_extent = dict(spatial_extent)

        cube = connection.load_collection(collection_name, spatial_extent, temporal_extent, bands)

        # Apply if the collection is a GeoJSON Feature collection
        if isinstance(spatial_extent, GeoJSON):
            cube = cube.filter_spatial(spatial_extent)

        return cube

    return s2_l2a_fetch_default


def get_s2_l2a_default_processor(collection_name: str) -> Callable:
    """Builds the preprocessing function from the collection name as it stored
    in the target backend.
    """

    def s2_l2a_default_processor(cube: openeo.DataCube, **params):
        """Default collection preprocessing method.
        This method performs reprojection if specified, upsampling of bands
        at 10m resolution as well as band reprojection.
        """
        # Reproject collection data to target CRS, if specified so
        cube = resample_reproject(cube, 10.0, params.get("target_crs", None))

        cube = rename_bands(cube, BAND_MAPPINGS.get(collection_name))

        return cube

    return s2_l2a_default_processor


SENTINEL2_L2A_BACKEND_MAP = {
    Backend.TERRASCOPE: {
        "fetch": get_s2_l2a_default_fetcher("SENTINEL2_L2A"),
        "preprocessor": get_s2_l2a_default_processor("SENTINEL2_L2A"),
    }
}


def build_sentinel2_l2a_extractor(
    backend_context: BackendContext, bands: list, **params
) -> CollectionExtractor:
    """Creates a S2 L2A extractor adapted to the given backend."""
    backend_functions = SENTINEL2_L2A_BACKEND_MAP.get(backend_context.backend)

    fetcher, preprocessor = (
        backend_functions["fetch"],
        backend_functions["preprocessor"],
    )

    return CollectionExtractor(backend_context, bands, fetcher, preprocessor, **params)
