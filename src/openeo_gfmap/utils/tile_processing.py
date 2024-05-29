"""Utilitaries to process data tiles."""

import numpy as np
import xarray as xr


def normalize_array(inarr: xr.DataArray, percentile: float = 0.99) -> xr.DataArray:
    """Performs normalization between 0.0 and 1.0 using the given
    percentile.
    """
    quantile_value = inarr.quantile(percentile, dim=["x", "y", "t"])
    minimum = inarr.min(dim=["x", "y", "t"])

    inarr = (inarr - minimum) / (quantile_value - minimum)

    # Perform clipping on values that are higher than the computed quantile
    return inarr.where(inarr < 1.0, 1.0)


def select_optical_bands(inarr: xr.DataArray) -> xr.DataArray:
    """Filters and keep only the optical bands for a given array."""
    return inarr.sel(
        bands=[
            band
            for band in inarr.coords["bands"].to_numpy()
            if band.startswith("S2-L2A-B")
        ]
    )


def select_sar_bands(inarr: xr.DataArray) -> xr.DataArray:
    """Filters and keep only the SAR bands for a given array."""
    return inarr.sel(
        bands=[
            band
            for band in inarr.coords["bands"].to_numpy()
            if band in ["S1-SIGMA0-VV", "S1-SIGMA0-VH", "S1-SIGMA0-HH", "S1-SIGMA0-HV"]
        ]
    )


def array_bounds(inarr: xr.DataArray) -> tuple:
    """Returns the 4 bounds values for the x and y coordinates of the tile"""
    return (
        inarr.coords["x"].min().item(),
        inarr.coords["y"].min().item(),
        inarr.coords["x"].max().item(),
        inarr.coords["y"].max().item(),
    )


def arrays_cosine_similarity(
    first_array: xr.DataArray, second_array: xr.DataArray
) -> float:
    """Returns a similarity score based on normalized cosine distance. The
    input arrays must have similar ranges to obtain a valid score.
    1.0 represents the best score (same tiles), while 0.0 is the worst score.
    """
    dot_product = np.sum(first_array * second_array)
    first_norm = np.linalg.norm(first_array)
    second_norm = np.linalg.norm(second_array)
    similarity = (dot_product / (first_norm * second_norm)).item()

    return similarity
