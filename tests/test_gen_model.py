"""Tests for `pydata_factory` package."""
from pathlib import Path

import pytest

from pydata_factory.classes import GenModel
from pydata_factory.schema import Schema


@pytest.mark.parametrize("schema_name", ["fb2021", "msft2021"])
def test_create_model_yahoo(schema_name):
    """Test the creation of a new model from a parquet file."""
    path = Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
    schema = Schema.load_file(path)
    result = GenModel.generate(schema)
    assert "class" in result


@pytest.mark.parametrize("schema_name", ["clients", "projects", "tasks"])
def test_create_model_tasks(schema_name):
    """Test the creation of a new model from a parquet file."""
    path = Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
    schema = Schema.load_file(path)
    result = GenModel.generate(schema)
    assert "class" in result
