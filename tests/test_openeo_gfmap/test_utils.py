import os
from pathlib import Path

import pystac
import pytest
from netCDF4 import Dataset

from openeo_gfmap import Backend, BackendContext, BoundingBoxExtent, TemporalContext
from openeo_gfmap.utils import split_collection_by_epsg, update_nc_attributes
from openeo_gfmap.utils.catalogue import s1_area_per_orbitstate, select_S1_orbitstate

# Region of Paris, France
SPATIAL_CONTEXT = BoundingBoxExtent(
    west=1.979, south=48.705, east=2.926, north=49.151, epsg=4326
)

# Summer 2023
TEMPORAL_CONTEXT = TemporalContext(start_date="2023-06-21", end_date="2023-09-21")


def test_query_cdse_catalogue():
    backend_context = BackendContext(Backend.CDSE)

    response = s1_area_per_orbitstate(
        backend=backend_context,
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

    # Testing the decision maker, it should return DESCENDING
    decision = select_S1_orbitstate(
        backend=backend_context,
        spatial_extent=SPATIAL_CONTEXT,
        temporal_extent=TEMPORAL_CONTEXT,
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
    split_collection_by_epsg(path=input_dir, output_dir=output_dir)

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
        split_collection_by_epsg(path=input_dir, output_dir=output_dir)
