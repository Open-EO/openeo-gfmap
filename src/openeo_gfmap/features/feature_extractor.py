"""Feature extractor functionalities. Such as a base class to assist the
implementation of feature extractors of a UDF.

Be careful: a lot of
  __    __  ______  ______  ______  ______  ______  ______  ______  ______  ______  __    __  __    __  __  __   __  ______
 /\ "-./  \/\  ___\/\__  _\/\  __ \/\  == \/\  == \/\  __ \/\  ___\/\  == \/\  __ \/\ "-./  \/\ "-./  \/\ \/\ "-.\ \/\  ___\
 \ \ \-./\ \ \  __\\/_/\ \/\ \  __ \ \  _-/\ \  __<\ \ \/\ \ \ \__ \ \  __<\ \  __ \ \ \-./\ \ \ \-./\ \ \ \ \ \-.  \ \ \__ \
  \ \_\ \ \_\ \_____\ \ \_\ \ \_\ \_\ \_\   \ \_\ \_\ \_____\ \_____\ \_\ \_\ \_\ \_\ \_\ \ \_\ \_\ \ \_\ \_\ \_\\"\_\ \_____\
   \/_/  \/_/\/_____/  \/_/  \/_/\/_/\/_/    \/_/ /_/\/_____/\/_____/\/_/ /_/\/_/\/_/\/_/  \/_/\/_/  \/_/\/_/\/_/ \/_/\/_____/

in here
"""

REQUIRED_IMPORTS = """
import inspect
from abc import ABC, abstractmethod

import openeo
from openeo.udf import XarrayDataCube
from openeo.udf.run_code import execute_local_udf
from openeo.udf.udf_data import UdfData

import xarray as xr
import numpy as np

from typing import Union
"""

exec(REQUIRED_IMPORTS)

LAT_HARMONIZED_NAME = "GEO-LAT"
LON_HARMONIZED_NAME = "GEO-LON"
EPSG_HARMONIZED_NAME = "GEO-EPSG"

APPLY_DATACUBE_SOURCE_CODE = """
from openeo.udf import XarrayDataCube
from openeo.udf.udf_data import UdfData

IS_PIXEL_BASED = {}

def apply_udf_data(udf_data: UdfData, context: dict) -> XarrayDataCube:
    feature_extractor = {}()  # User-defined, feature extractor class initialized here

    if not IS_PIXEL_BASED:
        assert len(udf_data.datacube_list) == 1, "OpenEO GFMAP Feature extractor pipeline only supports single input cubes for the tile."

    cube = udf_data.datacube_list[0]
    proj = udf_data.proj
    parameters = udf_data.user_context

    parameters['{}'] = proj['epsg']

    cube = feature_extractor._execute(cube, parameters=context)

    udf_data.datacube_list = [cube]
    
    return udf_data
"""

class FeatureExtractor(ABC):
    """Base class for all feature extractor UDFs. It provides some common
    methods and attributes to be used by other feature extractor.

    The inherited classes are supposed to take care of VectorDataCubes for
    point based extraction or dense Cubes for tile/polygon based extraction.
    """

    @abstractmethod
    def _import_dependencies(self):
        """Imports the dependencies that will be used in the user's feature
        extractor. This method will be execture at the very beginning of the
        process. Dependencies not imported here will not be loaded.
        """
        raise NotImplementedError(
            "FeatureExtractor is a base abstract class, please implement the "
            "function in your user defined feature extractor class."
        )

    def _common_preparations(
        self, inarr: xr.DataArray, parameters: dict
    ) -> xr.DataArray:
        """Common preparations to be executed before the feature extractor is
        executed. This method should be called by the `_execute` method of the
        feature extractor.
        """
        self._import_dependencies()
        self._epsg = parameters.pop(EPSG_HARMONIZED_NAME)
        self._parameters = parameters
        return inarr

    @property
    def epsg(self) -> int:
        """Returns the EPSG code of the datacube."""
        return self._epsg

    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        raise NotImplementedError(
            "FeatureExtractor is a base abstract class, please implement the "
            "_execute method."
        )


class PatchFeatureExtractor(FeatureExtractor):
    """Base class for all the tile/polygon based feature extractors. An user
    implementing a feature extractor should take care of
    """

    def get_latlons(self, inarr: xr.DataArray) -> xr.DataArray:
        """Returns the latitude and longitude coordinates of the given array in
        a dataarray. Returns a dataarray with the same width/height of the input
        array, but with two bands, one for latitude and one for longitude.

        The latitude and longitude band names are standardized to the names
        `LAT_HARMONIZED_NAME` and `LON_HARMONIZED_NAME` respectively.
        """
        lat = inarr.coords["y"]
        lon = inarr.coords["x"]

        # Create a two channel numpy array of the lat and lons together with
        # meshgrid
        latlon = np.stack([lat, lon])
        latlon = np.meshgrid(*latlon)

        # Repack in a dataarray
        return xr.DataArray(
            latlon,
            dims=["bands", "y", "x"],
            coords={
                "bands": [LAT_HARMONIZED_NAME, LON_HARMONIZED_NAME],
                "y": lat,
                "x": lon,
            },
        )

    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        arr = cube.get_array().transpose("bands", "t", "y", "x")
        arr = self._common_preparations(arr, parameters)
        arr = self.execute(arr)
        return XarrayDataCube(arr)

    @abstractmethod
    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        pass


class PointFeatureExtractor(FeatureExtractor):
    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        arr = cube.get_array().transpose("bands", "t")

        arr = self._common_preparations(arr, parameters)

        outarr = self.execute(cube.to_array()).transpose("bands", "t")
        return XarrayDataCube(outarr)

    @abstractmethod
    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        pass


def generate_udf_code(
    feature_extractor_class: FeatureExtractor
) -> openeo.UDF:
    """Generates the udf code by packing imports of this file, the necessary
    superclass and subclasses as well as the user defined feature extractor
    class and the apply_datacube function.
    """
    # UDF code that will be built here
    udf_code = ""

    assert issubclass(
        feature_extractor_class, FeatureExtractor
    ), "The feature extractor class must be a subclass of FeatureExtractor."

    if issubclass(feature_extractor_class, PatchFeatureExtractor):
        udf_code += f"{REQUIRED_IMPORTS}\n\n"
        udf_code += f"{inspect.getsource(FeatureExtractor)}\n\n"
        udf_code += f"{inspect.getsource(PatchFeatureExtractor)}\n\n"
        udf_code += f"{inspect.getsource(feature_extractor_class)}\n\n"
        udf_code += APPLY_DATACUBE_SOURCE_CODE.format(
            False,
            feature_extractor_class.__name__,
            EPSG_HARMONIZED_NAME
        )
    elif issubclass(feature_extractor_class, PointFeatureExtractor):
        udf_code += f"{REQUIRED_IMPORTS}\n\n"
        udf_code += f"{inspect.getsource(FeatureExtractor)}\n\n"
        udf_code += f"{inspect.getsource(PointFeatureExtractor)}\n\n"
        udf_code += f"{inspect.getsource(feature_extractor_class)}\n\n"
        udf_code += APPLY_DATACUBE_SOURCE_CODE.format(
            True,
            feature_extractor_class.__name__,
            EPSG_HARMONIZED_NAME
        )
    else:
        raise NotImplementedError(
            "The feature extractor must be a subclass of either "
            "PatchFeatureExtractor or PointFeatureExtractor."
        )

    return udf_code


def apply_feature_extractor(
    feature_extractor_class: FeatureExtractor,
    cube: openeo.rest.datacube.DataCube,
    parameters: dict,
    size: list,
    overlap: list = [],
) -> openeo.rest.datacube.DataCube:
    """Applies an user-defined feature extractor on the cube by using the
    `openeo.Cube.apply_neighborhood` method. The defined class as well as the
    required subclasses will be packed into a generated UDF file that will be
    executed.

    Optimization can be achieved by passing integer values for the cube. By
    default, the feature extractor expects to receive S1 and S2 data stored in
    uint16 with the

    """

    udf_code = generate_udf_code(feature_extractor_class)

    udf = openeo.UDF(code=udf_code, context=parameters)

    return cube.apply_neighborhood(process=udf, size=size, overlap=overlap)


def apply_feature_extractor_local(
    feature_extractor_class: FeatureExtractor,
    cube: xr.DataArray,
    parameters: dict
) -> xr.DataArray:
    """Applies and user-define feature extractor, but locally. The
    parameters are the same as in the `apply_feature_extractor` function,
    excepts for the cube parameter which expects a `xarray.DataArray` instead of
    a `openeo.rest.datacube.DataCube` object.
    """
    udf_code = generate_udf_code(feature_extractor_class)

    udf = openeo.UDF(code=udf_code, context=parameters)

    cube = XarrayDataCube(cube)

    out_udf_data: UdfData = execute_local_udf(
        udf, cube, fmt="NetCDF"
    )

    output_cubes = out_udf_data.datacube_list

    assert len(output_cubes) == 1, "UDF should have only a single output cube."

    return output_cubes[0].get_array()
