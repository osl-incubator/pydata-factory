"""Tests for `pydata_factory` package."""
from pathlib import Path

import pytest

from pydata_factory.class_model import create_model
from pydata_factory.schema import load_schema


@pytest.mark.parametrize("schema_name", ["fb2021", "msft2021"])
def test_create_model(schema_name):
    """Test the creation of a new model from a parquet file."""
    path = Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
    schema = load_schema(path)
    result = create_model(schema)
    assert "class" in result
