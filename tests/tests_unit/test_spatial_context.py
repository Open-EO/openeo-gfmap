from openeo_gfmap.spatial import BoundingBoxExtent


class TestBoundingBoxExtent:
    def test_basic(self):
        bbox = BoundingBoxExtent(1, 2, 3, 4)
        assert bbox.west == 1
        assert bbox.south == 2
        assert bbox.east == 3
        assert bbox.north == 4
        assert bbox.epsg == 4326
