"""Inference functionalities. Such as a base class to assist the implementation
of inference models on an UDF.
"""

import functools
import inspect
import re
import sys
from abc import ABC, abstractmethod

import numpy as np
import openeo
import requests
import xarray as xr
from openeo.udf import XarrayDataCube
from openeo.udf import inspect as udf_inspect
from openeo.udf.run_code import execute_local_udf
from openeo.udf.udf_data import UdfData

sys.path.insert(0, "onnx_deps")
import onnxruntime as ort  # noqa: E402

EPSG_HARMONIZED_NAME = "GEO-EPSG"


class ModelInference(ABC):
    """Base class for all model inference UDFs. It provides some common
    methods and attributes to be used by other model inference classes.
    """

    @functools.lru_cache(maxsize=6)
    def load_ort_session(self, model_url: str):
        """Loads an onnx session from a publicly available URL. The URL must be a direct
        download link to the ONNX session file.
        The `lru_cache` decorator avoids loading multiple time the model within the same worker.
        """
        # Two minutes timeout to download the model
        response = requests.get(model_url, timeout=120)
        model = response.content

        return ort.InferenceSession(model)

    def apply_ml(
        self, tensor: np.ndarray, session: ort.InferenceSession, input_name: str
    ) -> np.ndarray:
        """Applies the machine learning model to the input data as a tensor.

        Parameters
        ----------
        tensor: np.ndarray
            The input data with shape (bands, instance). If the input data is a tile (bands, y, x),
            then the y, x dimension must be flattened before being applied in this function.
        session: ort.InferenceSession
            The ONNX Session object, loaded from the `load_ort_session` class method.
        input_name: str
            The name of the input tensor in the ONNX session. Depends on how is the ONNX serialized
            model generated. For example, CatBoost models have their input tensor named as
            features: https://catboost.ai/en/docs/concepts/apply-onnx-ml
        """
        return session.run(None, {input_name: tensor})[0]

    def _common_preparations(self, inarr: xr.DataArray, parameters: dict) -> xr.DataArray:
        """Common preparations for all inference models. This method will be
        executed at the very beginning of the process.
        """
        self._epsg = parameters.pop(EPSG_HARMONIZED_NAME)
        self._parameters = parameters
        return inarr

    def _execute(self, cube: XarrayDataCube, parameters: dict) -> XarrayDataCube:
        arr = cube.get_array().transpose("bands", "y", "x")
        arr = self._common_preparations(arr, parameters)
        arr = self.execute(arr).transpose("bands", "y", "x")
        return XarrayDataCube(arr)

    @property
    def epsg(self) -> int:
        """EPSG code of the input data."""
        return self._epsg

    @abstractmethod
    def output_labels(self) -> list:
        """Returns the labels of the output data."""
        raise NotImplementedError(
            "ModelInference is a base abstract class, please implement the "
            "output_labels property."
        )

    @abstractmethod
    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        """Executes the model inference."""
        raise NotImplementedError(
            "ModelInference is a base abstract class, please implement the " "execute method."
        )


class ONNXModelInference(ModelInference):
    """Basic implementation of model inference that loads an ONNX model and runs the data
    through it. The input data, as model inference classes, is expected to have ('bands', 'y', 'x')
    as dimension orders, where 'bands' are the features that were computed the same way as for the
    training data.

    The following parameters are necessary:
    - `model_url`: URL to download the ONNX model.
    - `input_name`: Name of the input tensor in the ONNX model.
    - `output_labels`: Labels of the output data.

    """

    def output_labels(self) -> list:
        return self._parameters["output_labels"]

    def execute(self, inarr: xr.DataArray) -> xr.DataArray:
        if self._parameters.get("model_url") is None:
            raise ValueError("The model_url must be defined in the parameters.")

        # Load the model and the input_name parameters
        session = self.load_ort_session(self._parameters.get("model_url"))

        input_name = self._parameters.get("input_name")
        if input_name is None:
            input_name = session.get_inputs()[0].name
            udf_inspect(
                message=f"Input name not defined. Using name of parameters from the model session: {input_name}.",
                level="warning",
            )

        # Run the model inference on the input data
        input_data = inarr.values.astype(np.float32)
        n_bands, height, width = input_data.shape

        # Flatten the x and y coordiantes into one
        input_data = input_data.reshape(n_bands, -1).T

        # Make the prediction
        output = self.apply_ml(input_data, session, input_name)

        output = output.reshape(len(self.output_labels()), height, width)

        return xr.DataArray(
            output,
            dims=["bands", "y", "x"],
            coords={"bands": self.output_labels(), "x": inarr.x, "y": inarr.y},
        )


def apply_udf_data(udf_data: UdfData) -> XarrayDataCube:
    model_inference_class = "<model_inference_class>"

    model_inference = model_inference_class()

    # User-defined, model inference class initialized here
    cube = udf_data.datacube_list[0]
    parameters = udf_data.user_context

    proj = udf_data.proj
    if proj is not None:
        proj = proj.get("EPSG")

    parameters[EPSG_HARMONIZED_NAME] = proj

    cube = model_inference._execute(cube, parameters=parameters)

    udf_data.datacube_list = [cube]

    return udf_data


def _get_imports() -> str:
    with open(__file__, "r", encoding="UTF-8") as f:
        script_source = f.read()

    lines = script_source.split("\n")

    imports = []
    static_globals = []

    for line in lines:
        if line.strip().startswith(("import ", "from ", "sys.path.insert(", "sys.path.append(")):
            imports.append(line)
        elif re.match("^[A-Z_0-9]+\s*=.*$", line):
            static_globals.append(line)

    return "\n".join(imports) + "\n\n" + "\n".join(static_globals)


def _get_apply_udf_data(model_inference: ModelInference) -> str:
    source_lines = inspect.getsource(apply_udf_data)
    source = "".join(source_lines)
    # replace in the source function the `model_inference_class`
    return source.replace('"<model_inference_class>"', model_inference.__name__)


def _generate_udf_code(model_inference_class: ModelInference) -> openeo.UDF:
    """Generates the udf code by packing imports of this file, the necessary
    superclass and subclasses as well as the user defined model inference
    class and the apply_datacube function.
    """

    # UDF code that will be built here
    udf_code = ""

    assert issubclass(
        model_inference_class, ModelInference
    ), "The model inference class must be a subclass of ModelInference."

    udf_code += _get_imports() + "\n\n"
    udf_code += f"{inspect.getsource(ModelInference)}\n\n"
    udf_code += f"{inspect.getsource(model_inference_class)}\n\n"
    udf_code += _get_apply_udf_data(model_inference_class)
    return udf_code


def apply_model_inference(
    model_inference_class: ModelInference,
    cube: openeo.rest.datacube.DataCube,
    parameters: dict,
    size: list,
    overlap: list = [],
) -> openeo.rest.datacube.DataCube:
    """Applies an user-defined model inference on the cube by using the
    `openeo.Cube.apply_neighborhood` method. The defined class as well as the
    required subclasses will be packed into a generated UDF file that will be
    executed.
    """
    model_inference = model_inference_class()
    model_inference._parameters = parameters
    output_labels = model_inference.output_labels()

    udf_code = _generate_udf_code(model_inference_class)

    udf = openeo.UDF(code=udf_code, context=parameters)

    cube = cube.apply_neighborhood(process=udf, size=size, overlap=overlap)
    return cube.rename_labels(dimension="bands", target=output_labels)


def apply_model_inference_local(
    model_inference_class: ModelInference, cube: xr.DataArray, parameters: dict
) -> xr.DataArray:
    """Applies and user-defined model inference, but locally. The
    parameters are the same as in the `apply_model_inference` function,
    excepts for the cube parameter which expects a `xarray.DataArray` instead of
    a `openeo.rest.datacube.DataCube` object.
    """
    model_inference = model_inference_class()
    model_inference._parameters = parameters
    output_labels = model_inference.output_labels()

    udf_code = _generate_udf_code(model_inference_class)

    udf = openeo.UDF(code=udf_code, context=parameters)

    cube = XarrayDataCube(cube)

    out_udf_data: UdfData = execute_local_udf(udf, cube, fmt="NetCDF")

    output_cubes = out_udf_data.datacube_list

    assert len(output_cubes) == 1, "UDF should have only a single output cube."

    return output_cubes[0].get_array().assign_coords({"bands": output_labels})
