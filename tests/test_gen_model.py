"""Tests for `pydata_factory` package."""
from pathlib import Path

from pydata_factory.class_model import create_model


def test_create_model():
    """Test the creation of a new model from a parquet file."""
    path = Path(__file__).parent / "data" / "fb2021.parquet"
    result = create_model(str(path))
    assert result != ""
