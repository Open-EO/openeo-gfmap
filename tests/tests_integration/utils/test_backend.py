import os

import pytest

from openeo_gfmap.backend import Backend, get_connection


@pytest.mark.skipif(
    os.environ.get("SKIP_INTEGRATION_TESTS") == "1", reason="Skip integration tests"
)
@pytest.mark.parametrize("backend", [Backend.CDSE, Backend.TERRASCOPE])
def test_backend_connection(backend):
    con = get_connection(backend)
    info = con.describe_account()
    assert "user_id" in info, f"Failed to connect to {backend.name} backend."
