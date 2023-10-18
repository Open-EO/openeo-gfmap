from openeo_gfmap.spatial_context import BoundingBoxExtent


class TestBoundingBoxExtent:
    def test_basic(self):
        bbox = BoundingBoxExtent(1, 2, 3, 4)
        assert bbox.minx == 1
        assert bbox.miny == 2
        assert bbox.maxx == 3
        assert bbox.maxy == 4
        assert bbox.epsg == 4326
