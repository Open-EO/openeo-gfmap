from unittest.mock import MagicMock

import numpy as np
import onnxruntime as ort
import pytest
import requests
from pathlib import Path
import xarray as xr


from openeo_gfmap.inference.model_inference import(
    ONNXModelInference,
    ModelInference,
    apply_udf_data,
    apply_model_inference_local,
    UdfData)

MODEL_URL = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/knn_model_rgbnir.onnx"
BASE_URL = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/openeo"
TARGET_PATH = Path.cwd() / "dependencies"
DEPENDENCY_NAME = "onnx_dependencies_1.16.3.zip"
OUTPUT_LABELS = ["label1", "label2"]

@pytest.fixture
def mock_load_ort_session(requests_mock):
    response_content = b"mock_model_content"  # Example mock content
    requests_mock.get(MODEL_URL, content=response_content)


# Define the function to test
def load_ort_session(model_url: str):
    import onnxruntime as ort  # Import onnxruntime here as well

    # Load the model
    response = requests.get(model_url)
    model = response.content

    # Load the dependency into an InferenceSession
    session = ort.InferenceSession(model)

    # Return the ONNX runtime session loaded with the model
    return session


def test_load_ort_session():
    """
    Test the _load_ort_session function to ensure it correctly loads an ONNX model and initializes the ONNX runtime session.
    """

    session = load_ort_session(MODEL_URL)
    assert isinstance(session, ort.InferenceSession)


def test_extract_dependencies():
    model_inference = ONNXModelInference()
    extracted_path = model_inference.extract_dependencies(BASE_URL, DEPENDENCY_NAME)
     # Check if the extracted_path is correct
    assert isinstance(extracted_path, str)

    # Check if the TARGET_PATH exists and contains extracted files
    assert TARGET_PATH.resolve().exists()
    assert any(TARGET_PATH.resolve().glob("*"))


def test_output_labels():
    parameters = {"output_labels": OUTPUT_LABELS}
    model_inference = ONNXModelInference()
    model_inference._parameters = parameters
    assert model_inference.output_labels() == OUTPUT_LABELS

def test_apply_ml(mock_load_ort_session):
    inference = ONNXModelInference()
    mock_session = MagicMock()
    mock_session.run.return_value = [np.array([[0.1, 0.2, 0.7]])]  # Example output
    input_data = np.array([[1, 2, 3]])

    output = inference.apply_ml(input_data, mock_session, input_name="X")

    # Perform assertions on the output
    assert output.shape == (1, 3)  # Example assertion


def test_execute():
    model_inference = ONNXModelInference()

    model_inference = ONNXModelInference()
    model_inference._parameters = {"model_url": MODEL_URL, "input_name": "X", "output_labels": ["label"]}

    # Define dummy data matching the specified xarray.Dataset structure
    x_vals = np.linspace(0, 10, 10)
    y_vals = np.linspace(0, 10, 10)
    bands = ['S2-L2A-B04', 'S2-L2A-B08', 'S2-L2A-B11', 'S2-L2A-B12', 'S2-L2A-NDVI']
    data = np.ones([len(bands), len(y_vals), len(x_vals)])

    # Create the dummy dataset
    coords = {'x': x_vals, 'y': y_vals, 'bands': bands}
    dims = ['bands', 'y', 'x']

    # Create xr.DataArray with input_data_np, dims, and coords
    input_data_xr = xr.DataArray(data, dims=dims, coords=coords)

    # Call execute method with xr.DataArray input
    output = model_inference.execute(input_data_xr)
    
    # Add assertions to validate output
    assert isinstance(output, xr.DataArray)

