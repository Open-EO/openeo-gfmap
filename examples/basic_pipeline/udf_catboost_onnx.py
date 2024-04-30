import functools
import numpy as np
from openeo.udf import inspect
import requests
import sys
from typing import Dict
import xarray as xr

# Import the onnxruntime package from the onnx_deps directory
sys.path.insert(0, "onnx_deps")
import onnxruntime as ort 


@functools.lru_cache(maxsize=6)
def _load_ort_session(model_url: str):
    """
    Load the models and make the prediction functions.
    The lru_cache avoids loading the model multiple times on the same worker.
    """
    inspect(message=f"Loading random forrest as ONNX runtime session ...")
    response = requests.get(model_url)
    model = response.content
    inspect(message=f"Model loaded from {model_url}", level="debug")
    return ort.InferenceSession(model)


def _apply_ml(tensor : np.ndarray, session: ort.InferenceSession, input_name) -> np.ndarray:
    """
    Apply the model to a tensor containing features.
    """
    tensor = tensor.reshape((1,) + tensor.shape)
    return session.run(None, {input_name: tensor})[0]

def apply_datacube(cube: xr.DataArray, context: Dict) -> xr.DataArray:
    """
    Apply the model to the datacube.
    """
    # Load the model
    session = _load_ort_session(context.get("model_url", None))
    input_name=session.get_inputs()[0].name

    # Prepare the input
    input_data = cube.values.astype(np.float32)

    # Make the prediction
    output = np.apply_along_axis(lambda x: _apply_ml(x, session, input_name), axis=0, arr=input_data)

    # Prepare the output
    output = output.reshape(cube.shape[1:])
    return xr.DataArray(output).astype("uint8")

