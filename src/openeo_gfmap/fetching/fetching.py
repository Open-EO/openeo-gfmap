""" Main file for extractions and pre-processing of data through OpenEO
"""

from enum import Enum
from typing import Callable

import openeo

from openeo_gfmap import BackendContext
from openeo_gfmap.spatial import SpatialContext
from openeo_gfmap.temporal import TemporalContext


class FetchType(Enum):
    """Enumerates the different types of extraction. There are three types of
    enumerations.

    * TILE: Tile based extractions, getting the data for a dense part. The
    output of such fetching process in a dense DataCube.
    * POINT: Point based extractions. From a datasets of polygons, gets sparse
    extractions and performs spatial aggregation on the selected polygons. The
    output of such fetching process is a VectorCube, that can be used to get
    a pandas.DataFrame
    * POLYGON: Patch based extractions, returning a VectorCube of sparsed
    patches. This can be retrieved as multiple NetCDF files from one job.
    """

    TILE = "tile"
    POINT = "point"
    POLYGON = "polygon"


class CollectionFetcher:
    """Base class to fetch a particular collection.

    Parameters
    ----------
    backend_context: BackendContext
        Information about the backend in use, useful in certain cases.
    bands: list
        List of band names to load from that collection.
    collection_fetch: Callable
        Function defining how to fetch a collection for a specific backend,
        the function accepts the following parameters: connection,
        spatial extent, temporal extent, bands and additional parameters.
    collection_preprocessing: Callable
        Function defining how to harmonize the data of a collection in a
        backend. For example, this function could rename the bands as they
        can be different for every backend/collection (SENTINEL2_L2A or
        SENTINEL2_L2A_SENTINELHUB). Accepts the following parameters:
        datacube (of pre-fetched collection) and additional parameters.
    colection_params: dict
        Additional parameters encoded within a dictionnary that will be
        passed in the fetch and preprocessing function.
    """

    def __init__(
        self,
        backend_context: BackendContext,
        bands: list,
        collection_fetch: Callable,
        collection_preprocessing: Callable,
        **collection_params,
    ):
        self.backend_contect = backend_context
        self.bands = bands
        self.fetcher = collection_fetch
        self.processing = collection_preprocessing
        self.params = collection_params

    def get_cube(
        self,
        connection: openeo.Connection,
        spatial_context: SpatialContext,
        temporal_context: TemporalContext,
    ) -> openeo.DataCube:
        """Retrieve a data cube from the given spatial and temporal context.

        Parameters
        ----------
        connection: openeo.Connection
            A connection to an OpenEO backend. The backend provided must be the
            same as the one this extractor class is configured for.
        spatial_extent: SpatialContext
            Either a GeoJSON collection on which spatial filtering will be
            applied or a bounding box with an EPSG code. If a bounding box is
            provided, no filtering is applied and the entirety of the data is
            fetched for that region.
        temporal_extent: TemporalContext
            The begin and end date of the extraction.
        """
        collection_data = self.fetcher(
            connection, spatial_context, temporal_context, self.bands, **self.params
        )

        preprocessed_data = self.processing(collection_data, **self.params)

        return preprocessed_data
