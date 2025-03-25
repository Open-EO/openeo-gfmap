from pathlib import Path
from typing import List, Optional

import openeo
import pytest
from openeo.rest._testing import build_capabilities
from openeo.rest.connection import Connection
from openeo.rest.datacube import DataCube
from openeo.testing import TestDataLoader

API_URL = "https://oeo.test"

DEFAULT_S1_METADATA = {
    "cube:dimensions": {
        "x": {"type": "spatial"},
        "y": {"type": "spatial"},
        "t": {"type": "temporal"},
        "bands": {"type": "bands", "values": ["VV", "VH"]},
    },
    "summaries": {
        "eo:bands": [
            {"name": "VV"},
            {"name": "VH"},
        ]
    },
}


@pytest.fixture
def test_data() -> TestDataLoader:
    return TestDataLoader(root=Path(__file__).parent / "data")


@pytest.fixture(params=["1.0.0"])
def api_version(request):
    return request.param


@pytest.fixture
def api_capabilities() -> dict:
    """
    Fixture to be overridden for customizing the capabilities doc used by connection fixtures.
    To be used as kwargs for `build_capabilities`
    """
    return {}


def _setup_connection(
    api_version, requests_mock, build_capabilities_kwargs: Optional[dict] = None
) -> Connection:
    requests_mock.get(
        API_URL + "/",
        json=build_capabilities(
            api_version=api_version, **(build_capabilities_kwargs or {})
        ),
    )
    # Alias for quick tests
    requests_mock.get(API_URL + "/collections/S1", json=DEFAULT_S1_METADATA)

    requests_mock.get(
        API_URL + "/file_formats",
        json={
            "output": {
                "GTiff": {"gis_data_types": ["raster"]},
                "netCDF": {"gis_data_types": ["raster"]},
                "csv": {"gis_data_types": ["table"]},
            }
        },
    )
    requests_mock.get(
        API_URL + "/udf_runtimes",
        json={
            "Python": {
                "type": "language",
                "default": "3",
                "versions": {"3": {"libraries": {}}},
            },
            "R": {
                "type": "language",
                "default": "4",
                "versions": {"4": {"libraries": {}}},
            },
        },
    )

    return openeo.connect(API_URL)


def setup_collection_metadata(requests_mock, cid: str, bands: List[str]):
    """Set up mock collection metadata"""
    requests_mock.get(
        API_URL + f"/collections/{cid}",
        json={
            "cube:dimensions": {"bands": {"type": "bands", "values": bands}},
            "summaries": {"eo:bands": [{"name": b} for b in bands]},
        },
    )


@pytest.fixture
def connection(api_version, requests_mock, api_capabilities) -> Connection:
    """Connection fixture to a backend of given version with some image collections."""
    return _setup_connection(
        api_version, requests_mock, build_capabilities_kwargs=api_capabilities
    )


@pytest.fixture
def con100(requests_mock, api_capabilities) -> Connection:
    """Connection fixture to a 1.0.0 backend with some image collections."""
    return _setup_connection(
        "1.0.0", requests_mock, build_capabilities_kwargs=api_capabilities
    )


@pytest.fixture
def s1cube(connection, api_version) -> DataCube:
    return connection.load_collection("S1")
