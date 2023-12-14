from pathlib import Path

from typing import Union

import pytest

from openeo_gfmap.backend import Backend, BackendContext, BACKEND_CONNECTIONS
from openeo_gfmap.temporal import TemporalContext
from openeo_gfmap.spatial import BoundingBoxExtent
from openeo_gfmap.fetching import FetchType, build_sentinel2_l2a_extractor

from openeo_gfmap.preprocessing import (
    get_bap_score, bap_masking, median_compositing
)

backends = [Backend.TERRASCOPE, Backend.CDSE]

# Few fields around Mol, Belgium
spatial_extent = BoundingBoxExtent(
    west=5.0515130512706845,
    south=51.215806593713,
    east=5.060320484557499,
    north=51.22149744530769,
    epsg=4326
)

# November 2022 to February 2023
temporal_extent = TemporalContext(
    start_date="2022-10-31", end_date="2023-03-01"
)

@pytest.mark.parametrize("backend", backends)
def test_bap_score(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)

    # Additional parameters
    fetching_parameters = {
        "fetching_resolution": 10.0
    }

    preprocessing_parameters = {
        "apply_scl_dilation": True
    }

    # Fetch the datacube
    s2_extractor = build_sentinel2_l2a_extractor(
        backend_context=backend_context,
        bands=["S2-B04", "S2-B08", "S2-SCL"],
        fetch_type=FetchType.TILE,
        **fetching_parameters
    )

    cube = s2_extractor.get_cube(
        connection, spatial_extent, temporal_extent
    )

    # Compute the BAP score
    bap_score = get_bap_score(cube, **preprocessing_parameters)
    ndvi = cube.ndvi(nir="S2-B08", red="S2-B04")
    
    cube = bap_score.merge_cubes(ndvi).rename_labels(
        'bands', ['S2-BAPSCORE', 'S2-NDVI']
    )

    job = cube.create_job(
        title="BAP score unittest",
        out_format="NetCDF",
    )

    job.start_and_wait()
    
    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(
                Path(__file__).parent / f"results/bap_score_{backend.value}.nc"
            )

@pytest.mark.parametrize("backend", backends)
def test_bap_masking(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)

    # Additional parameters
    fetching_parameters = {
        "fetching_resolution": 10.0
    }

    # Fetch the datacube
    s2_extractor = build_sentinel2_l2a_extractor(
        backend_context=backend_context,
        bands=["S2-B04", "S2-B03", "S2-B02", "S2-SCL"],
        fetch_type=FetchType.TILE,
        **fetching_parameters
    )

    cube = s2_extractor.get_cube(
        connection, spatial_extent, temporal_extent
    )

    cube = cube.linear_scale_range(0, 65535, 0, 65535)

    # Perform masking with BAP, masking optical bands 
    cube = bap_masking(cube, period="dekad")

    # Perform compositing, the cube should be only composed of optical bands
    cube = median_compositing(cube, period="dekad")

    cube = cube.linear_scale_range(0, 65535, 0, 65535)

    # Remove SCL
    cube = cube.filter_bands(
        [band for band in cube.metadata.band_names if band != "S2-SCL"]
    )

    job = cube.create_job(
        title="BAP compositing unittest",
        out_format="NetCDF",
    )

    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(
                Path(__file__).parent / f"results/bap_composited_{backend.value}.nc"
            )

