""" Tests for data extractors for Sentinel2 data. """
from tempfile import NamedTemporaryFile

import geojson
import openeo
import pytest
import rioxarray

from openeo_gfmap.backend import Backend, BackendContext
from openeo_gfmap.extractions import CollectionExtractor, build_sentinel2_l2a_extractor

backends = [Backend.TERRASCOPE]

SPATIAL_EXTENT = geojson.GeoJSON(
    {
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-1.1658592364939022, 46.148659939867414],
                    [-1.1652611878225363, 46.16665089074196],
                    [-1.1393678791090696, 46.16623223621429],
                    [-1.1399743561415463, 46.14824154662241],
                    [-1.1658592364939022, 46.148659939867414],
                ]
            ],
        },
        "crs": "EPSG:4326",
    }
)

TEMPORAL_EXTENT = ["2020-04-01", "2020-08-01"]


@pytest.fixture
def vito_connection(capfd):
    # Note: this generic `authenticate_oidc()` call allows both:
    # - device code/refresh token based authentication for manual test
    #   suiteruns by a developer
    # - client credentials auth through env vars for automated/Jenkins CI runs
    #
    # See https://open-eo.github.io/openeo-python-client/auth.html#oidc-authentication-dynamic-method-selection  # NOQA
    # and Jenkinsfile, where Jenkins fetches the env vars from VITO TAP Vault.
    connection = openeo.connect("openeo.vito.be")
    with capfd.disabled():
        # Temporarily disable output capturing, to make sure that OIDC device
        # code instructions (if any) are shown.
        connection.authenticate_oidc()
    return connection


class TestS2Extractors:
    """Build collection extractor for different S2 collections on different
    backends.
    """

    def sentinel2_l2a(backend: Backend, vito_connection: openeo.Connection):
        context = BackendContext(backend)
        # Fetch a variety of spatial resolution and metadata from different
        # providers.
        bands = ["B01", "B04", "B08", "B11", "SCL", "CLP", "CLM", "AOT"]
        expected_harmonized_names = [
            "S2-B01",
            "S2-B04",
            "S2-B08",
            "S2-B11",
            "S2-SCL",
            "s2cloudless-CLP",
            "s2cloudless-CLM",
            "S2-AOT",
        ]
        fetching_parameters = {"target_crs": 3035}
        extractor: CollectionExtractor = build_sentinel2_l2a_extractor(
            context, bands, **fetching_parameters
        )

        cube = extractor.get_cube(vito_connection, SPATIAL_EXTENT, TEMPORAL_EXTENT)

        output_file = NamedTemporaryFile()

        cube.download(output_file.name, format="NetCDF")

        # Load the job results
        results = rioxarray.open_rasterio(output_file.name)

        output_file.close()

        print(results)

        # Assert the bands are renamed
        assert results.rio.crs == "EPSG:4326"

        for harmonized_name in expected_harmonized_names:
            assert harmonized_name is results.coords["bands"]


@pytest.mark.parametrize("backend", backends)
def test_sentinel2_l2a(backend, vito_connection):
    TestS2Extractors.sentinel2_l2a(backend, vito_connection)
