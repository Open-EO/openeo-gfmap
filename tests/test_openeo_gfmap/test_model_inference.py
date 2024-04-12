"""Test on model inference implementations, both local and remote."""
from pathlib import Path

import numpy as np
import xarray as xr
from openeo.udf import XarrayDataCube

from openeo_gfmap import (
    Backend,
    BackendContext,
    BoundingBoxExtent,
    FetchType,
    TemporalContext,
)
from openeo_gfmap.backend import cdse_connection
from openeo_gfmap.fetching.s2 import build_sentinel2_l2a_extractor
from openeo_gfmap.inference.model_inference import (
    ONNXModelInference,
    apply_model_inference,
)
from openeo_gfmap.preprocessing.cloudmasking import mask_scl_dilation
from openeo_gfmap.preprocessing.compositing import median_compositing

spatial_context = BoundingBoxExtent(west=5.0, south=51.2, east=5.025, north=51.225, epsg=4326)

temporal_extent = TemporalContext(start_date="2018-05-01", end_date="2018-10-31")

onnx_model_url = (
    "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/knn_model_rgbnir.onnx"
)
dependency_url = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/openeo/onnx_dependencies_1.16.3.zip"


def test_onnx_inference_local():
    """Test the ONNX Model inference locally"""
    inds = xr.open_dataarray(Path(__file__).parent / "resources/test_inference_feats.nc")

    inference = ONNXModelInference()

    # Waiting for this ticker to be implemented properly
    # https://github.com/Open-EO/openeo-python-client/issues/556
    output = inference._execute(
        XarrayDataCube(inds),
        parameters={
            "model_url": onnx_model_url,
            "input_name": "X",
            "output_labels": ["label"],
            "GEO-EPSG": "whatever",
        },
    )

    output = output.get_array()

    # Assert that 3 output classes are present
    assert output.shape == (1, 256, 256)
    assert len(np.unique(output.values)) == 3

    output_path = Path(__file__).parent / "results/test_onnx_inference_local.nc"
    output.to_netcdf(output_path)


def test_onnx_inference():
    """Simple test on the ONNX Model Inference class"""
    connection = cdse_connection()

    bands = [
        "S2-L2A-B04",
        "S2-L2A-B03",
        "S2-L2A-B02",
        "S2-L2A-B08",
        "S2-L2A-SCL",
    ]

    fetcher = build_sentinel2_l2a_extractor(
        backend_context=BackendContext(Backend.CDSE_STAGING),
        bands=bands,
        fetch_type=FetchType.TILE,
    )

    cube = fetcher.get_cube(connection, spatial_context, temporal_extent)

    # Perform some cloud-masking and monthly median compositing
    cube = mask_scl_dilation(cube)
    cube = median_compositing(cube, period="year")

    # We remove the SCL mask
    cube = cube.filter_bands(bands=["S2-L2A-B04", "S2-L2A-B03", "S2-L2A-B02", "S2-L2A-B08"])

    cube = cube.ndvi(nir="S2-L2A-B08", red="S2-L2A-B04", target_band="S2-L2A-NDVI")

    # Perform model inference
    cube = apply_model_inference(
        model_inference_class=ONNXModelInference,
        cube=cube,
        parameters={
            "model_url": onnx_model_url,
            "input_name": "X",
            "output_labels": ["label"],
        },
        size=[
            {"dimension": "x", "unit": "px", "value": 128},
            {"dimension": "y", "unit": "px", "value": 128},
            {"dimension": "t", "value": 1},
        ],
    )

    output_path = Path(__file__).parent / "results/test_onnx_inference.tif"

    # Download the results as tif file.
    job = cube.create_job(
        title="test_onnx_inference",
        out_format="GTiff",
        job_options={
            "udf-dependency-archives": [f"{dependency_url}#onnx_deps"],
        },
    )
    job.start_and_wait()

    for asset in job.get_results().get_assets():
        if asset.metadata["type"].startswith("application/x-netcdf"):
            asset.download(output_path)
            break

    assert output_path.exists()

    inds = xr.open_dataset(output_path).to_array(dim="bands")

    assert inds.shape == (1, 256, 256)
    assert len(np.unique(inds.values)) == 3
