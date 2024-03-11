from pathlib import Path

import pytest

from openeo_gfmap.backend import BACKEND_CONNECTIONS, Backend, BackendContext
from openeo_gfmap.fetching import FetchType, build_sentinel2_l2a_extractor
from openeo_gfmap.preprocessing import (
    bap_masking,
    get_bap_mask,
    get_bap_score,
    median_compositing,
)
from openeo_gfmap.spatial import BoundingBoxExtent
from openeo_gfmap.temporal import TemporalContext
from openeo_gfmap.utils import quintad_intervals

backends = [Backend.TERRASCOPE, Backend.CDSE]

# Few fields around Mol, Belgium
spatial_extent = BoundingBoxExtent(
    west=5.0515130512706845,
    south=51.215806593713,
    east=5.060320484557499,
    north=51.22149744530769,
    epsg=4326,
)

# November 2022 to February 2023
temporal_extent = TemporalContext(start_date="2022-11-01", end_date="2023-02-28")


@pytest.mark.parametrize("backend", backends)
def test_bap_score(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)

    # Additional parameters
    fetching_parameters = {"fetching_resolution": 10.0}

    preprocessing_parameters = {"apply_scl_dilation": True}

    # Fetch the datacube
    s2_extractor = build_sentinel2_l2a_extractor(
        backend_context=backend_context,
        bands=["S2-L2A-B04", "S2-L2A-B08", "S2-L2A-SCL"],
        fetch_type=FetchType.TILE,
        **fetching_parameters,
    )

    cube = s2_extractor.get_cube(connection, spatial_extent, temporal_extent)

    # Compute the BAP score
    bap_score = get_bap_score(cube, **preprocessing_parameters)
    ndvi = cube.ndvi(nir="S2-L2A-B08", red="S2-L2A-B04")

    cube = bap_score.merge_cubes(ndvi).rename_labels("bands", ["S2-L2A-BAPSCORE", "S2-L2A-NDVI"])

    job = cube.create_job(
        title="BAP score unittest",
        out_format="NetCDF",
    )

    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(Path(__file__).parent / f"results/bap_score_{backend.value}.nc")


@pytest.mark.parametrize("backend", backends)
def test_bap_masking(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)

    # Additional parameters
    fetching_parameters = {"fetching_resolution": 10.0}

    # Fetch the datacube
    s2_extractor = build_sentinel2_l2a_extractor(
        backend_context=backend_context,
        bands=["S2-L2A-B04", "S2-L2A-B03", "S2-L2A-B02", "S2-L2A-SCL"],
        fetch_type=FetchType.TILE,
        **fetching_parameters,
    )

    cube = s2_extractor.get_cube(connection, spatial_extent, temporal_extent)

    cube = cube.linear_scale_range(0, 65535, 0, 65535)

    # Perform masking with BAP, masking optical bands
    cube = bap_masking(cube, period="dekad")

    # Perform compositing, the cube should be only composed of optical bands
    cube = median_compositing(cube, period="dekad")

    cube = cube.linear_scale_range(0, 65535, 0, 65535)

    # Remove SCL
    cube = cube.filter_bands([band for band in cube.metadata.band_names if band != "S2-L2A-SCL"])

    job = cube.create_job(
        title="BAP compositing unittest",
        out_format="NetCDF",
    )

    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(Path(__file__).parent / f"results/bap_composited_{backend.value}.nc")


@pytest.mark.parametrize("backend", backends)
def test_bap_quintad(backend: Backend):
    connection = BACKEND_CONNECTIONS[backend]()
    backend_context = BackendContext(backend=backend)

    # Additional parameters
    fetching_parameters = {"fetching_resolution": 10.0}
    preprocessing_parameters = {"apply_scl_dilation": True}

    # Fetch the datacube
    s2_extractor = build_sentinel2_l2a_extractor(
        backend_context=backend_context,
        bands=["S2-L2A-SCL"],
        fetch_type=FetchType.TILE,
        **fetching_parameters,
    )

    cube = s2_extractor.get_cube(connection, spatial_extent, temporal_extent)

    compositing_intervals = quintad_intervals(temporal_extent)

    expected_intervals = [
        ("2022-11-01", "2022-11-05"),
        ("2022-11-06", "2022-11-10"),
        ("2022-11-11", "2022-11-15"),
        ("2022-11-16", "2022-11-20"),
        ("2022-11-21", "2022-11-25"),
        ("2022-11-26", "2022-11-30"),
        ("2022-12-01", "2022-12-05"),
        ("2022-12-06", "2022-12-10"),
        ("2022-12-11", "2022-12-15"),
        ("2022-12-16", "2022-12-20"),
        ("2022-12-21", "2022-12-25"),
        ("2022-12-26", "2022-12-31"),
        ("2023-01-01", "2023-01-05"),
        ("2023-01-06", "2023-01-10"),
        ("2023-01-11", "2023-01-15"),
        ("2023-01-16", "2023-01-20"),
        ("2023-01-21", "2023-01-25"),
        ("2023-01-26", "2023-01-31"),
        ("2023-02-01", "2023-02-05"),
        ("2023-02-06", "2023-02-10"),
        ("2023-02-11", "2023-02-15"),
        ("2023-02-16", "2023-02-20"),
        ("2023-02-21", "2023-02-25"),
        ("2023-02-26", "2023-02-28"),
    ]

    assert compositing_intervals == expected_intervals

    # Perform masking with BAP, masking optical bands
    bap_mask = get_bap_mask(cube, period=compositing_intervals, **preprocessing_parameters)

    # Create a new extractor for the whole data now
    fetching_parameters = {
        "fetching_resolution": 10.0,
        "pre_mask": bap_mask,  # Use of the pre-computed bap mask to load inteligently the data
    }

    s2_extractor = build_sentinel2_l2a_extractor(
        backend_context=backend_context,
        bands=["S2-L2A-B04", "S2-L2A-B03", "S2-L2A-B02", "S2-L2A-B08", "S2-L2A-SCL"],
        fetch_type=FetchType.TILE,
        **fetching_parameters,
    )

    # Performs quintal compositing
    cube = s2_extractor.get_cube(connection, spatial_extent, temporal_extent)

    cube = median_compositing(cube, period=compositing_intervals)

    cube = cube.linear_scale_range(0, 65535, 0, 65535)

    job = cube.create_job(
        title="BAP optimized fetching",
        out_format="NetCDF",
    )

    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(Path(__file__).parent / f"results/bap_quintad_{backend.value}.nc")
