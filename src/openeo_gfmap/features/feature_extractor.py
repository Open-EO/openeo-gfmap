"""Feature extractor functionalities. Such as a base class to assist the
implementation of feature extractors of a UDF.
"""

import functools
import inspect
import logging
import re
import shutil
import sys
import urllib.request
from abc import ABC, abstractmethod
from pathlib import Path

import numpy as np
import openeo
import xarray as xr
from openeo.udf import XarrayDataCube
from openeo.udf.udf_data import UdfData
from pyproj import Transformer
from pyproj.crs import CRS

sys.path.append("feature_deps")

LAT_HARMONIZED_NAME = "GEO-LAT"
LON_HARMONIZED_NAME = "GEO-LON"
EPSG_HARMONIZED_NAME = "GEO-EPSG"


class FeatureExtractor(ABC):
    """Base class for all feature extractor UDFs. It provides some common
    methods and attributes to be used by other feature extractor.

    The inherited classes are supposed to take care of VectorDataCubes for
    point based extraction or dense Cubes for tile/polygon based extraction.
    """

    def __init__(self) -> None:
        self._epsg = None

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    @functools.lru_cache(maxsize=6)
    def extract_dependencies(cls, base_url: str, dependency_name: str) -> str:
        """Extract the dependencies from the given URL. Unpacking a zip
        file in the current working directory and return the path to the
        unpacked directory.

        Parameters:
        - base_url: The base public URL where the dependencies are stored.
        - dependency_name: The name of the dependency file to download. This
            parameter is added to `base_url` as a download path to the .zip
            archive
        Returns:
        - The absolute path to the extracted dependencies directory, to be added
            to the python path with the `sys.path.append` method.
        """

        # Generate absolute path for the dependencies folder
        dependencies_dir = Path.cwd() / "dependencies"

        # Create the directory if it doesn't exist
        dependencies_dir.mkdir(exist_ok=True, parents=True)

        # Download and extract the model file
        modelfile_url = f"{base_url}/{dependency_name}"
        modelfile, _ = urllib.request.urlretrieve(
            modelfile_url, filename=dependencies_dir / Path(modelfile_url).name
        )
        shutil.unpack_archive(modelfile, extract_dir=dependencies_dir)

        # Add the model directory to system path if it's not already there
        abs_path = str(
            dependencies_dir / Path(modelfile_url).name.split(".zip")[0]
        )  # NOQA

        return abs_path

    def _common_preparations(
        self, inarr: xr.DataArray, parameters: dict
    ) -> xr.DataArray:
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

    @epsg.setter
    def epsg(self, value: int):
        self._epsg = value

    def dependencies(self) -> list:
        """Returns the additional dependencies such as wheels or zip files.
        Dependencies should be returned as a list of string, which will set-up at the top of the
        generated UDF. More information can be found at:
        https://open-eo.github.io/openeo-python-client/udf.html#standard-for-declaring-python-udf-dependencies
        """
        self.logger.warning(
            "No additional dependencies are defined. If you wish to add "
            "dependencies to your feature extractor, override the "
            "`dependencies` method in your class."
        )
        return []

    @abstractmethod
    def output_labels(self) -> list:
        """Returns a list of output labels to be assigned on the output bands,
        needs to be overriden by the user."""
        raise NotImplementedError(
            "FeatureExtractor is a base abstract class, please implement the "
            "output_labels property."
        )

    @abstractmethod
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
                "EPSG code was not defined, cannot extract lat/lon array "
                "as the CRS is unknown."
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

    def _rescale_s1_backscatter(self, arr: xr.DataArray) -> xr.DataArray:
        """Rescales the input array from uint16 to float32 decibel values.
        The input array should be in uint16 format, as this optimizes memory usage in Open-EO
        processes. This function is called automatically on the bands of the input array, except
        if the parameter `rescale_s1` is set to False.
        """
        s1_bands = ["S1-SIGMA0-VV", "S1-SIGMA0-VH", "S1-SIGMA0-HV", "S1-SIGMA0-HH"]
        s1_bands_to_select = list(set(arr.bands.values) & set(s1_bands))

        if len(s1_bands_to_select) == 0:
            return arr

        data_to_rescale = arr.sel(bands=s1_bands_to_select).astype(np.float32).data

        # Assert that the values are set between 1 and 65535
        if data_to_rescale.min().item() < 1 or data_to_rescale.max().item() > 65535:
            raise ValueError(
                "The input array should be in uint16 format, with values between 1 and 65535. "
                "This restriction assures that the data was processed according to the S1 fetcher "
                "preprocessor. The user can disable this scaling manually by setting the "
                "`rescale_s1` parameter to False in the feature extractor."
            )

        # Converting back to power values
        data_to_rescale = 20.0 * np.log10(data_to_rescale) - 83.0
        data_to_rescale = np.power(10, data_to_rescale / 10.0)
        data_to_rescale[~np.isfinite(data_to_rescale)] = np.nan

        # Converting power values to decibels
        data_to_rescale = 10.0 * np.log10(data_to_rescale)

        # Change the bands within the array
        arr.loc[dict(bands=s1_bands_to_select)] = data_to_rescale
        return arr

    # TODO to remove the fixed transpose as it contributes to unclear code.
    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        arr = cube.get_array().transpose("bands", "t", "y", "x")
        arr = self._common_preparations(arr, parameters)
        if self._parameters.get("rescale_s1", True):
            arr = self._rescale_s1_backscatter(arr)

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
        if line.strip().startswith(
            ("import ", "from ", "sys.path.insert(", "sys.path.append(")
        ):
            imports.append(line)
        elif re.match("^[A-Z_0-9]+\s*=.*$", line):
            static_globals.append(line)

    return "\n".join(imports) + "\n\n" + "\n".join(static_globals)


def _get_apply_udf_data(feature_extractor: FeatureExtractor) -> str:
    source_lines = inspect.getsource(apply_udf_data)
    source = "".join(source_lines)
    # replace in the source function the `feature_extractor_class`
    return source.replace('"<feature_extractor_class>"', feature_extractor.__name__)


def _generate_udf_code(
    feature_extractor_class: FeatureExtractor, dependencies: list
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

    dependencies_code = ""
    dependencies_code += "# /// script\n"
    dependencies_code += "# dependencies = [\n"
    for dep in dependencies:
        dependencies_code += f'#  "{dep}",\n'
    dependencies_code += "# ]\n"
    dependencies_code += "# ///\n"

    udf_code += dependencies_code + "\n"
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
    dependencies = feature_extractor.dependencies()

    udf_code = _generate_udf_code(feature_extractor_class, dependencies)

    udf = openeo.UDF(code=udf_code, context=parameters)

    cube = cube.apply_neighborhood(process=udf, size=size, overlap=overlap)
    return cube.rename_labels(dimension="bands", target=output_labels)


def apply_feature_extractor_local(
    feature_extractor_class: FeatureExtractor, cube: xr.DataArray, parameters: dict
) -> xr.DataArray:
    """Applies and user-defined feature extractor, but locally. The
    parameters are the same as in the `apply_feature_extractor` function,
    excepts for the cube parameter which expects a `xarray.DataArray` instead of
    a `openeo.rest.datacube.DataCube` object.
    """
    # Trying to get the local EPSG code
    if EPSG_HARMONIZED_NAME not in parameters:
        raise ValueError(
            f"Please specify an EPSG code in the parameters with key: {EPSG_HARMONIZED_NAME} when "
            f"running a Feature Extractor locally."
        )

    feature_extractor = feature_extractor_class()
    feature_extractor._parameters = parameters
    output_labels = feature_extractor.output_labels()
    dependencies = feature_extractor.dependencies()

    if len(dependencies) > 0:
        feature_extractor.logger.warning(
            "Running UDFs locally with pip dependencies is not supported yet, "
            "dependencies will not be installed."
        )

    cube = XarrayDataCube(cube)

    return (
        feature_extractor._execute(cube, parameters)
        .get_array()
        .assign_coords({"bands": output_labels})
    )
