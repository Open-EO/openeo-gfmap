import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

import geojson
import pandas as pd
import pystac
import pytest
from netCDF4 import Dataset

from openeo_gfmap import BoundingBoxExtent, TemporalContext, _BackendGroup
from openeo_gfmap.utils import split_collection_by_epsg, update_nc_attributes
from openeo_gfmap.utils.catalogue import (
    _compute_max_gap_days,
    s1_area_per_orbitstate_vvvh,
    select_s1_orbitstate_vvvh,
)

# Region of Paris, France
SPATIAL_CONTEXT = BoundingBoxExtent(
    west=1.979, south=48.705, east=2.926, north=49.151, epsg=4326
)

# Summer 2023
TEMPORAL_CONTEXT = TemporalContext(start_date="2023-06-21", end_date="2023-09-21")


def mock_query_cdse_catalogue(
    collection: str,
    bounds: list,
    temporal_extent: TemporalContext,
    **additional_parameters: dict,
):
    """Mocks the results of the CDSE catalogue query by computing the hash of the input parameters
    and returning the results from a resource file if it exists.
    """
    # Compute the hash of the input parameters
    arguments = "".join([str(x) for x in [collection, bounds, temporal_extent]])
    kw_arguments = "".join(
        [f"{key}{value}" for key, value in additional_parameters.items()]
    )
    combined_arguments = arguments + kw_arguments
    hash_value = hashlib.sha256(combined_arguments.encode()).hexdigest()

    src_path = (
        Path(__file__).parent.parent
        / f"resources/{hash_value[:8]}_query_cdse_results.json"
    )

    if not src_path.exists():
        raise ValueError("No cached results for the given parameters.")

    return json.loads(src_path.read_text())


@patch("openeo_gfmap.utils.catalogue._query_cdse_catalogue", mock_query_cdse_catalogue)
def test_query_cdse_catalogue():
    response = s1_area_per_orbitstate_vvvh(
        backend=_BackendGroup.CDSE,
        spatial_extent=SPATIAL_CONTEXT,
        temporal_extent=TEMPORAL_CONTEXT,
    )

    assert response is not None

    # Checks the values for ASCENDING and DESCENDING
    assert "ASCENDING" in response.keys()
    assert "DESCENDING" in response.keys()

    assert response["ASCENDING"]["area"] > 0.0
    assert response["DESCENDING"]["area"] > 0.0

    assert response["ASCENDING"]["area"] < response["DESCENDING"]["area"]

    assert response["ASCENDING"]["full_overlap"] is True
    assert response["DESCENDING"]["full_overlap"] is True

    assert response["ASCENDING"]["max_temporal_gap"] > 0.0
    assert response["DESCENDING"]["max_temporal_gap"] > 0.0

    # Testing the decision maker, it should return DESCENDING
    decision = select_s1_orbitstate_vvvh(
        backend=_BackendGroup.CDSE,
        spatial_extent=SPATIAL_CONTEXT,
        temporal_extent=TEMPORAL_CONTEXT,
    )

    assert decision == "DESCENDING"


@patch("openeo_gfmap.utils.catalogue._query_cdse_catalogue", mock_query_cdse_catalogue)
def test_query_cdse_catalogue_with_s1_gap():
    """This example has a large S1 gap in ASCENDING,
    so the decision should be DESCENDING
    """

    spatial_extent = geojson.loads(
        (
            '{"features": [{"geometry": {"coordinates": [[[35.85799, 49.705688], [35.85799, 49.797363], [36.039682, 49.797363], '
            '[36.039682, 49.705688], [35.85799, 49.705688]]], "type": "Polygon"}, "id": "0", "properties": '
            '{"GT_available": true, "extract": 1, "index": 12, "sample_id": "ukraine_sunflower", "tile": '
            '"36UYA", "valid_time": "2019-05-01", "year": 2019}, "type": "Feature"}], "type": "FeatureCollection"}'
        )
    )
    temporal_extent = TemporalContext("2019-01-30", "2019-08-31")

    decision = select_s1_orbitstate_vvvh(
        _BackendGroup.CDSE, spatial_extent, temporal_extent
    )

    assert decision == "DESCENDING"


@pytest.fixture
def temp_nc_file():
    temp_file = Path("temp_test.nc")
    yield temp_file
    os.remove(temp_file)


def test_update_nc_attributes(temp_nc_file):
    test_attributes = {
        "one": "two",
        "three": "four",
        "changing_attribute": "changed_value",
    }

    with Dataset(temp_nc_file, "w") as nc:
        nc.setncattr("existing_attribute", "existing_value")
        nc.setncattr("changing_attribute", "changing_value")

    update_nc_attributes(temp_nc_file, test_attributes)

    with Dataset(temp_nc_file, "r") as nc:
        for attr_name, attr_value in test_attributes.items():
            assert attr_name in nc.ncattrs()
            assert getattr(nc, attr_name) == attr_value
        assert "existing_attribute" in nc.ncattrs()
        assert nc.getncattr("existing_attribute") == "existing_value"


def test_split_collection_by_epsg(tmp_path):
    collection = pystac.collection.Collection.from_dict(
        {
            "type": "Collection",
            "id": "test-collection",
            "stac_version": "1.0.0",
            "description": "Test collection",
            "links": [],
            "title": "Test Collection",
            "extent": {
                "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
                "temporal": {
                    "interval": [["2020-01-01T00:00:00Z", "2020-01-10T00:00:00Z"]]
                },
            },
            "license": "proprietary",
            "summaries": {"eo:bands": [{"name": "B01"}, {"name": "B02"}]},
        }
    )
    first_item = pystac.item.Item.from_dict(
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "4326-item",
            "properties": {
                "datetime": "2020-05-22T00:00:00Z",
                "eo:bands": [{"name": "SCL"}, {"name": "B08"}],
                "proj:epsg": 4326,
            },
            "geometry": {
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                "type": "Polygon",
            },
            "links": [],
            "assets": {},
            "bbox": [0, 1, 0, 1],
            "stac_extensions": [],
        }
    )
    second_item = pystac.item.Item.from_dict(
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "3857-item",
            "properties": {
                "datetime": "2020-05-22T00:00:00Z",
                "eo:bands": [{"name": "SCL"}, {"name": "B08"}],
                "proj:epsg": 3857,
            },
            "geometry": {
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                "type": "Polygon",
            },
            "links": [],
            "assets": {},
            "bbox": [0, 1, 0, 1],
            "stac_extensions": [],
        }
    )
    collection.add_items([first_item, second_item])
    input_dir = str(tmp_path / "collection.json")
    output_dir = str(tmp_path / "split_collections")

    collection.normalize_and_save(input_dir)
    split_collection_by_epsg(collection=input_dir, output_dir=output_dir)

    # Collection contains two different EPSG codes, so 2 collections should be created
    assert len([p for p in Path(output_dir).iterdir() if p.is_dir()]) == 2

    missing_epsg_item = pystac.item.Item.from_dict(
        {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "3857-item",
            "properties": {
                "datetime": "2020-05-22T00:00:00Z",
                "eo:bands": [{"name": "SCL"}, {"name": "B08"}],
            },
            "geometry": {
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
                "type": "Polygon",
            },
            "links": [],
            "assets": {},
            "bbox": [0, 1, 0, 1],
            "stac_extensions": [],
        }
    )

    # Collection contains item without EPSG, so KeyError should be raised
    with pytest.raises(KeyError):
        collection.add_item(missing_epsg_item)
        collection.normalize_and_save(input_dir)
        split_collection_by_epsg(collection=input_dir, output_dir=output_dir)


@patch("openeo_gfmap.utils.catalogue._query_cdse_catalogue", mock_query_cdse_catalogue)
def test_compute_max_gap():
    start_date = "2020-01-01"
    end_date = "2020-01-31"

    temporal_context = TemporalContext(start_date, end_date)

    resulting_dates = [
        "2020-01-03",
        "2020-01-05",
        "2020-01-10",
        "2020-01-25",
        "2020-01-26",
        "2020-01-27",
    ]

    resulting_dates = [
        pd.to_datetime(date, format="%Y-%m-%d", utc=True) for date in resulting_dates
    ]

    max_gap = _compute_max_gap_days(temporal_context, resulting_dates)

    assert max_gap == 15
