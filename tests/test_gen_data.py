"""Tests for `pydata_factory` package."""
from pathlib import Path

import pytest

from pydata_factory.data import GenData
from pydata_factory.schema import Schema


@pytest.mark.parametrize("schema_name", ["fb2021", "msft2021"])
def test_gen_data_individually(schema_name):
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
    schema = Schema.load_file(origin)
    schemas = {schema["name"]: schema}
    df = GenData.generate(schemas)[Schema.get_qualified_name(schema)]
    assert not df.empty


def test_gen_data_yahoo_batch():
    """Test the creation of a new model from a parquet file."""
    schemas = {}

    for schema_name in ["fb2021", "msft2021"]:
        schema_path = (
            Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
        )
        schema = Schema.load_file(schema_path)
        schemas[schema["name"]] = schema

    dfs = GenData.generate(schemas)

    for k, df in dfs.items():
        assert not df.empty


def test_gen_data_tasks_batch():
    """Test the creation of a new model from a parquet file."""
    schemas = {}

    for schema_name in ["clients", "projects"]:
        schema_path = (
            Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
        )
        schema = Schema.load_file(schema_path)
        schemas[schema["name"]] = schema

    dfs = GenData.generate(schemas)

    for k, df in dfs.items():
        assert not df.empty
