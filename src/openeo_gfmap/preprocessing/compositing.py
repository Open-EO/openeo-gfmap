"""Temporal compositing, or temporal aggregation, is a method to increase the
quality of data within timesteps by reducing the temporal resolution of a time
series of satellite images.
"""
from typing import Union

import openeo


def median_compositing(cube: openeo.DataCube, period: Union[str, list]) -> openeo.DataCube:
    """Perfrom median compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(period=period, reducer="median", dimension="t")
    elif isinstance(period, list):
        return cube.aggregate_temporal(intervals=period, reducer="median", dimension="t")


def mean_compositing(cube: openeo.DataCube, period: str) -> openeo.DataCube:
    """Perfrom mean compositing on the given datacube."""
    if isinstance(period, str):
        return cube.aggregate_temporal_period(period=period, reducer="mean", dimension="t")
    elif isinstance(period, list):
        return cube.aggregate_temporal(intervals=period, reducer="mean", dimension="t")
