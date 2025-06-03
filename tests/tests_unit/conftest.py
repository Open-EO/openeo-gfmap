import pytest
from openeo.rest._testing import DummyBackend

from openeo_gfmap.backend import _BackendGroup

API_URL = _BackendGroup.TEST.default_url


@pytest.fixture
def api_capabilities() -> dict:
    """
    Fixture to be overridden for customizing the capabilities doc used by connection fixtures.
    To be used as kwargs for `build_capabilities`

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client
    """
    return {}


# @pytest.fixture
# def con_client_creds(requests_mock):
#     """
#     Fixture to create a connection to the dummy backend using openeo 1.2.0.
#     """
#     client_id = "test_client_id"
#     client_secret = "test_client_secret"
#     issuer_name = "fauth"
#     issuer_link = "https://fauth.test"

#     os.environ["OPENEO_AUTH_CLIENT_ID_TEST"] = client_id
#     os.environ["OPENEO_AUTH_CLIENT_SECRET_TEST"] = client_secret
#     os.environ["OPENEO_AUTH_PROVIDER_ID_TEST"] = issuer_name
#     os.environ["OPENEO_AUTH_METHOD"] = "client_credentials"

#     requests_mock.get(
#         API_URL + "credentials/oidc",
#         json={
#             "providers": [
#                 {
#                     "id": issuer_name,
#                     "issuer": issuer_link,
#                     "title": "Foo Auth",
#                     "scopes": ["openid", "im"],
#                 }
#             ]
#         },
#     )
#     OidcMock(
#         requests_mock=requests_mock,
#         expected_grant_type="client_credentials",
#         expected_client_id=client_id,
#         expected_fields={"client_secret": client_secret, "scope": "openid"},
#         oidc_issuer=issuer_link,
#     )

#     con = Connection(API_URL)
#     return con


@pytest.fixture
def dummy_backend(requests_mock, con120) -> DummyBackend:
    """
    Fixture to create a dummy backend for testing.

    This backend is used to test the job manager with a dummy backend
    using the DummyBackend defined in openeo for testing purposes.

    Inspired by the tests in https://github.com/Open-EO/openeo-python-client
    """
    dummy_backend = DummyBackend(requests_mock=requests_mock, connection=con120)
    dummy_backend.setup_collection(
        "SENTINEL2_L2A",
        bands=[
            "B01",
            "B02",
            "B03",
            "B04",
            "B05",
            "B06",
            "B07",
            "B08",
            "B8A",
            "B09",
            "B11",
            "B12",
            "SCL",
        ],
    )
    dummy_backend.setup_file_format("GTiff")
    dummy_backend.setup_file_format("netCDF")
    return dummy_backend
