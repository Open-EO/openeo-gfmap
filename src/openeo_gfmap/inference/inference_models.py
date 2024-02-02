"""Inference functionalities. Such as a base class to assist the implementation
of inference models on an UDF.
"""

from abc import ABC

import xarray as xr

from openeo_gfmap.features.feature_extractor import EPSG_HARMONIZED_NAME

REQUIRED_IMPORTS = """
import inspect
from abc import ABC, abstractmethod

import openeo
from openeo.udf import XarrayDataCube
from openeo.udf.run_code import execute_local_udf
from openeo.udf.udf_data import UdfData

from openeo_gfmap.features.feature_extractor import EPSG_HARMONIZED_NAME

import xarray as xr
import numpy as np

from typing import Union
"""


class ModelInference(ABC):
    """Base class for all model inference UDFs. It provides some common
    methods and attributes to be used by other model inference classes.
    """

    def _common_preparations(
        self, inarr: xr.DataArray, parameters: dict
    ) -> xr.DataArray:
        """Common preparations for all inference models. This method will be
        executed at the very beginning of the process.
        """
        raise NotImplementedError("Inference UDF are not implemented yet.")
