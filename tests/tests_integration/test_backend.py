from openeo_gfmap.backend import cdse_connection, vito_connection


def test_vito_connection_auth():
    con = vito_connection()
    info = con.describe_account()
    assert "user_id" in info


def test_cdse_connection_auth():
    con = cdse_connection()
    info = con.describe_account()
    assert "user_id" in info
