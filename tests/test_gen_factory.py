"""Tests for `pydata_factory` package."""
from pathlib import Path

import pytest

from pydata_factory.classes import GenFactory
from pydata_factory.schema import Schema


@pytest.mark.parametrize("schema_name", ["fb2021", "msft2021"])
def test_create_factory_yahoo(schema_name):
    """Test the creation of a new model from a parquet file."""
    path = Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
    schema = Schema.load_file(path)
    schemas = {schema["name"]: schema}
    result = GenFactory.generate(schema, "__main__", schemas)
    assert "class" in result


def test_create_factory_tasks():
    """Test the creation of a new model from a parquet file."""
    schemas = {}
    for schema_name in ["clients", "projects", "tasks"]:
        path = (
            Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
        )
        schema = Schema.load_file(path)
        schemas[schema["name"]] = schema

    for k_schema, v_schema in schemas.items():
        result = GenFactory.generate(v_schema, "__main__", schemas)
        assert "class" in result
