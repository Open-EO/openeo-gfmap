import requests
import numpy as np
import pytest
import onnxruntime as ort

from openeo_gfmap.inference.model_inference import ONNXModelInference
from unittest.mock import MagicMock


model_url = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/gfmap/knn_model_rgbnir.onnx"


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

    session = load_ort_session(model_url)
    assert isinstance(session, ort.InferenceSession)


@pytest.fixture
def mock_load_ort_session(requests_mock):
    response_content = b"mock_model_content"  # Example mock content
    requests_mock.get(model_url, content=response_content)


def test_apply_ml(mock_load_ort_session):
    inference = ONNXModelInference()
    mock_session = MagicMock()
    mock_session.run.return_value = [np.array([[0.1, 0.2, 0.7]])]  # Example output
    input_data = np.array([[1, 2, 3]])

    output = inference.apply_ml(input_data, mock_session, input_name="X")

    # Perform assertions on the output
    assert output.shape == (1, 3)  # Example assertion
