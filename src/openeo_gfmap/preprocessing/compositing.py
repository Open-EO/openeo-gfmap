"""Temporal compositing, or temporal aggregation, is a method to increase the
quality of data within timesteps by reducing the temporal resolution of a time
series of satellite images.
"""

from typing import Union

import openeo


def median_compositing(
    cube: openeo.DataCube, period: Union[str, list]
) -> openeo.DataCube:
    """Perfrom median compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(
            period=period, reducer="median", dimension="t"
        )
    elif isinstance(period, list):
        return cube.aggregate_temporal(
            intervals=period, reducer="median", dimension="t"
        )


def mean_compositing(
    cube: openeo.DataCube, period: Union[str, list]
) -> openeo.DataCube:
    """Perfrom mean compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(
            period=period, reducer="mean", dimension="t"
        )
    elif isinstance(period, list):
        return cube.aggregate_temporal(intervals=period, reducer="mean", dimension="t")


def sum_compositing(cube: openeo.DataCube, period: Union[str, list]) -> openeo.DataCube:
    """Perform sum compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(
            period=period, reducer="sum", dimension="t"
        )
    elif isinstance(period, list):
        return cube.aggregate_temporal(intervals=period, reducer="sum", dimension="t")


def max_ndvi_compositing(cube: openeo.DataCube, period: str) -> openeo.DataCube:
    """Perform compositing by selecting the observation with the highest NDVI value over the
    given compositing window."""

    def max_ndvi_selection(ndvi: openeo.DataCube):
        max_ndvi = ndvi.max()
        return ndvi.array_apply(lambda x: x != max_ndvi)

    if isinstance(period, str):
        ndvi = cube.ndvi(nir="S2-L2A-B08", red="S2-L2A-B04")

        rank_mask = ndvi.apply_neighborhood(
            max_ndvi_selection,
            size=[
                {"dimension": "x", "unit": "px", "value": 1},
                {"dimension": "y", "unit": "px", "value": 1},
                {"dimension": "t", "value": period},
            ],
            overlap=[],
        )

        cube = cube.mask(mask=rank_mask).aggregate_temporal_period(period, "first")

    else:
        raise ValueError(
            "Custom temporal intervals are not yet supported for max NDVI compositing."
        )
    return cube
