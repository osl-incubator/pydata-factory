"""Tests for `pydata_factory` package."""
from pathlib import Path

import pytest

from pydata_factory.data import gen_data_from_schema, gen_data_from_schemas
from pydata_factory.schema import load_schema


@pytest.mark.parametrize("schema_name", ["fb2021", "msft2021"])
def test_gen_data_from_schema(schema_name):
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"

    schema = load_schema(origin)
    df = gen_data_from_schema(schema)

    assert not df.empty


def test_gen_data_from_schemas():
    """Test the creation of a new model from a parquet file."""
    schemas = []

    for schema_name in ["fb2021", "msft2021"]:
        schema_path = (
            Path(__file__).parent / "data" / "schemas" / f"{schema_name}.json"
        )
        schema = load_schema(schema_path)
        schemas.append(schema)

    dfs = gen_data_from_schemas(schemas)

    for k, df in dfs.items():
        assert not df.empty
