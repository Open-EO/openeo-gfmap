"""Routines to pre-process sar signals."""
import openeo


def compress_backscatter_uint16():
    pass


def multitemporal_speckle(cube: openeo.DataCube) -> openeo.DataCube:
    _ = cube.filter_bands(
        bands=filter(lambda band: band.startswith("S1"), cube.metadata.band_names)
    )
    pass
