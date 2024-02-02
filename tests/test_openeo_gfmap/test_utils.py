from openeo_gfmap import Backend, BackendContext, BoundingBoxExtent, TemporalContext
from openeo_gfmap.utils.catalogue import s1_area_per_orbitstate

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
