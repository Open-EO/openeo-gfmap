"""Feature extractor functionalities. Such as a base class to assist the
implementation of feature extractors of a UDF.
"""

import inspect
import re
from abc import ABC, abstractmethod

import numpy as np
import openeo
import xarray as xr
from openeo.udf import XarrayDataCube
from openeo.udf.run_code import execute_local_udf
from openeo.udf.udf_data import UdfData
from pyproj import Transformer
from pyproj.crs import CRS

LAT_HARMONIZED_NAME = "GEO-LAT"
LON_HARMONIZED_NAME = "GEO-LON"
EPSG_HARMONIZED_NAME = "GEO-EPSG"


class FeatureExtractor(ABC):
    """Base class for all feature extractor UDFs. It provides some common
    methods and attributes to be used by other feature extractor.

    The inherited classes are supposed to take care of VectorDataCubes for
    point based extraction or dense Cubes for tile/polygon based extraction.
    """

    def _common_preparations(self, inarr: xr.DataArray, parameters: dict) -> xr.DataArray:
        """Common preparations to be executed before the feature extractor is
        executed. This method should be called by the `_execute` method of the
        feature extractor.
        """
        self._epsg = parameters.pop(EPSG_HARMONIZED_NAME)
        self._parameters = parameters
        return inarr

    @property
    def epsg(self) -> int:
        """Returns the EPSG code of the datacube."""
        return self._epsg

    @abstractmethod
    def output_labels(self) -> list:
        """Returns a list of output labels to be assigned on the output bands,
        needs to be overriden by the user."""
        raise NotImplementedError(
            "FeatureExtractor is a base abstract class, please implement the "
            "output_labels property."
        )

    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        raise NotImplementedError(
            "FeatureExtractor is a base abstract class, please implement the " "_execute method."
        )


class PatchFeatureExtractor(FeatureExtractor):
    """Base class for all the tile/polygon based feature extractors. An user
    implementing a feature extractor should take care of
    """

    def get_latlons(self, inarr: xr.DataArray) -> xr.DataArray:
        """Returns the latitude and longitude coordinates of the given array in
        a dataarray. Returns a dataarray with the same width/height of the input
        array, but with two bands, one for latitude and one for longitude. The
        metadata coordinates of the output array are the same as the input
        array, as the array wasn't reprojected but instead new features were
        computed.

        The latitude and longitude band names are standardized to the names
        `LAT_HARMONIZED_NAME` and `LON_HARMONIZED_NAME` respectively.
        """

        lon = inarr.coords["x"]
        lat = inarr.coords["y"]
        lon, lat = np.meshgrid(lon, lat)

        if self.epsg is None:
            raise Exception(
                "EPSG code was not defined, cannot extract lat/lon array " "as the CRS is unknown."
            )

        # If the coordiantes are not in EPSG:4326, we need to reproject them
        if self.epsg != 4326:
            # Initializes a pyproj reprojection object
            transformer = Transformer.from_crs(
                crs_from=CRS.from_epsg(self.epsg),
                crs_to=CRS.from_epsg(4326),
                always_xy=True,
            )
            lon, lat = transformer.transform(xx=lon, yy=lat)

        # Create a two channel numpy array of the lat and lons together by stacking
        latlon = np.stack([lat, lon])

        # Repack in a dataarray
        return xr.DataArray(
            latlon,
            dims=["bands", "y", "x"],
            coords={
                "bands": [LAT_HARMONIZED_NAME, LON_HARMONIZED_NAME],
                "y": inarr.coords["y"],
                "x": inarr.coords["x"],
            },
        )

    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        arr = cube.get_array().transpose("bands", "t", "y", "x")
        arr = self._common_preparations(arr, parameters)
        arr = self.execute(arr).transpose("bands", "y", "x")
        return XarrayDataCube(arr)

    @abstractmethod
    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        pass


class PointFeatureExtractor(FeatureExtractor):
    def __init__(self):
        raise NotImplementedError(
            "Point based feature extraction on Vector Cubes is not supported yet."
        )

    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        arr = cube.get_array().transpose("bands", "t")

        arr = self._common_preparations(arr, parameters)

        outarr = self.execute(cube.to_array()).transpose("bands", "t")
        return XarrayDataCube(outarr)

    @abstractmethod
    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        pass


def apply_udf_data(udf_data: UdfData) -> XarrayDataCube:
    feature_extractor_class = "<feature_extractor_class>"

    # User-defined, feature extractor class initialized here
    feature_extractor = feature_extractor_class()

    is_pixel_based = issubclass(feature_extractor_class, PointFeatureExtractor)

    if not is_pixel_based:
        assert (
            len(udf_data.datacube_list) == 1
        ), "OpenEO GFMAP Feature extractor pipeline only supports single input cubes for the tile."

    cube = udf_data.datacube_list[0]
    parameters = udf_data.user_context

    proj = udf_data.proj
    if proj is not None:
        proj = proj["EPSG"]

    parameters[EPSG_HARMONIZED_NAME] = proj

    cube = feature_extractor._execute(cube, parameters=parameters)

    udf_data.datacube_list = [cube]

    return udf_data


def _get_imports() -> str:
    with open(__file__, "r", encoding="UTF-8") as f:
        script_source = f.read()

    lines = script_source.split("\n")

    imports = []
    static_globals = []

    for line in lines:
        if line.strip().startswith(("import ", "from ")):
            imports.append(line)
        elif re.match("^[A-Z_0-9]+\s*=.*$", line):
            static_globals.append(line)

    return "\n".join(imports) + "\n\n" + "\n".join(static_globals)


def _get_apply_udf_data(feature_extractor: FeatureExtractor) -> str:
    source_lines = inspect.getsource(apply_udf_data)
    source = "".join(source_lines)
    # replace in the source function the `feature_extractor_class`
    return source.replace('"<feature_extractor_class>"', feature_extractor.__name__)


def generate_udf_code(feature_extractor_class: FeatureExtractor) -> openeo.UDF:
    """Generates the udf code by packing imports of this file, the necessary
    superclass and subclasses as well as the user defined feature extractor
    class and the apply_datacube function.
    """

    # UDF code that will be built here
    udf_code = ""

    assert issubclass(
        feature_extractor_class, FeatureExtractor
    ), "The feature extractor class must be a subclass of FeatureExtractor."

    udf_code += _get_imports() + "\n\n"
    udf_code += f"{inspect.getsource(FeatureExtractor)}\n\n"
    udf_code += f"{inspect.getsource(PatchFeatureExtractor)}\n\n"
    udf_code += f"{inspect.getsource(PointFeatureExtractor)}\n\n"
    udf_code += f"{inspect.getsource(feature_extractor_class)}\n\n"
    udf_code += _get_apply_udf_data(feature_extractor_class)
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
    uint16 with the harmonized naming as implemented in the fetching module.
    """
    feature_extractor = feature_extractor_class()
    feature_extractor._parameters = parameters
    output_labels = feature_extractor.output_labels()

    udf_code = generate_udf_code(feature_extractor_class)

    udf = openeo.UDF(code=udf_code, context=parameters)

    cube = cube.apply_neighborhood(process=udf, size=size, overlap=overlap)
    return cube.rename_labels(dimension="bands", target=output_labels)


def apply_feature_extractor_local(
    feature_extractor_class: FeatureExtractor, cube: xr.DataArray, parameters: dict
) -> xr.DataArray:
    """Applies and user-define feature extractor, but locally. The
    parameters are the same as in the `apply_feature_extractor` function,
    excepts for the cube parameter which expects a `xarray.DataArray` instead of
    a `openeo.rest.datacube.DataCube` object.
    """
    feature_extractor = feature_extractor_class()
    feature_extractor._parameters = parameters
    output_labels = feature_extractor.output_labels()

    udf_code = generate_udf_code(feature_extractor_class)

    udf = openeo.UDF(code=udf_code, context=parameters)

    cube = XarrayDataCube(cube)

    out_udf_data: UdfData = execute_local_udf(udf, cube, fmt="NetCDF")

    output_cubes = out_udf_data.datacube_list

    assert len(output_cubes) == 1, "UDF should have only a single output cube."

    return output_cubes[0].get_array().assign_coords({"bands": output_labels})
