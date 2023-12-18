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
from abc import ABC

import openeo
from openeo.udf import XarrayDataCube

import xarray as xr
import numpy as np
"""

exec(REQUIRED_IMPORTS)

APPLY_DATACUBE_SOURCE_CODE = """
from openeo.udf import XarrayDataCube

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    feature_extractor = {}()

    return feature_extractor._execute(cube, parameters=context)

"""

LAT_HARMONIZED_NAME = "GEO-LAT"
LON_HARMONIZED_NAME = "GEO-LON"


class FeatureExtractor(ABC):
    """Base class for all feature extractor UDFs. It provides some common
    methods and attributes to be used by other feature extractor.

    The inherited classes are supposed to take care of VectorDataCubes for
    point based extraction or dense Cubes for tile/polygon based extraction.
    """

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
        self._parameters = parameters
        return inarr

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
        arr = cube.to_array().transpose("bands", "t", "y", "x")
        arr = self._common_preparations(arr, parameters)
        arr = self.execute(arr)
        return XarrayDataCube(arr)

    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        raise NotImplementedError(
            "PatchFeatureExtractor is an abstract class, please implement the "
            "execute method."
        )


class PointFeatureExtractor(FeatureExtractor):
    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        arr = cube.to_array().transpose("bands", "t")

        arr = self._common_preparations(arr, parameters)

        outarr = self.execute(cube.to_array()).transpose("bands", "t")
        return XarrayDataCube(outarr)

    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        raise NotImplementedError(
            "PointFeatureExtractor is an abstract class, please implement the "
            "execute method."
        )


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
        udf_code += APPLY_DATACUBE_SOURCE_CODE.format(feature_extractor_class.__name__)
    elif issubclass(feature_extractor_class, PointFeatureExtractor):
        udf_code += f"{REQUIRED_IMPORTS}\n\n"
        udf_code += f"{inspect.getsource(FeatureExtractor)}\n\n"
        udf_code += f"{inspect.getsource(PointFeatureExtractor)}\n\n"
        udf_code += f"{inspect.getsource(feature_extractor_class)}\n\n"
        udf_code += APPLY_DATACUBE_SOURCE_CODE.format(feature_extractor_class.__name__)
    else:
        raise NotImplementedError(
            "The feature extractor must be a subclass of either "
            "PatchFeatureExtractor or PointFeatureExtractor."
        )

    udf = openeo.UDF(code=udf_code, context=parameters)

    return cube.apply_neighborhood(process=udf, size=size, overlap=overlap)
