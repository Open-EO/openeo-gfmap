import json
import os
from unittest.mock import MagicMock

import pytest

import openeo_gfmap
from openeo_gfmap.preprocessing.sar import (
    compress_backscatter_uint16,
    decompress_backscatter_uint16,
)


@pytest.fixture
def mock_backend_context():
    """Fixture to create a mock backend context."""
    return MagicMock(spec=openeo_gfmap.BackendContext)


def test_compress_backscatter_uint16(s1cube, mock_backend_context):
    cube = compress_backscatter_uint16(mock_backend_context, s1cube)
    test_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(test_dir, "data/compress_backscatter_uint16.json")
    with open(json_path) as f:
        expected = json.load(f)
    assert cube.flat_graph() == expected


def test_decompress_backscatter_uint16(s1cube, mock_backend_context):
    cube = decompress_backscatter_uint16(mock_backend_context, s1cube)
    test_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(test_dir, "data/decompress_backscatter_uint16.json")
    with open(json_path) as f:
        expected = json.load(f)
    assert cube.flat_graph() == expected
