"""Tests for `pydata_factory` package."""
from pathlib import Path

import pytest
import sqlalchemy as sqla

from pydata_factory.schema import Schema


@pytest.mark.parametrize("filename", ["fb2021.parquet", "msft2021.parquet"])
def test_schema_from_parquet(filename):
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "original" / filename
    target_dir = Path(__file__).parent / "data" / "schemas"
    schema = Schema.from_parquet(str(origin), str(target_dir))

    assert isinstance(schema, dict)
    assert "name" in schema
    assert "physical-name" in schema
    assert "attributes" in schema


@pytest.mark.parametrize("filename", ["db.sqlite"])
def test_schema_from_sql(filename):
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "original" / filename
    target_dir = Path(__file__).parent / "data" / "schemas"
    engine = sqla.create_engine(f"sqlite:///{origin}")
    schema = Schema.from_sql(
        engine=engine, table_name="fb2021", target_dir=str(target_dir)
    )

    assert isinstance(schema, dict)
    assert "name" in schema
    assert "physical-name" in schema
    assert "attributes" in schema


@pytest.mark.parametrize("filename", ["fb2021.parquet", "msft2021.parquet"])
def test_schema_from_parquet_with_namespace(filename):
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "original" / filename
    target_dir = Path(__file__).parent / "data" / "schemas"

    namespace = "pydf"

    schema = Schema.from_parquet(
        str(origin), str(target_dir), namespace=namespace
    )

    name = f"{filename.split('.')[0].title()}{namespace.title()}"

    assert schema["name"] == name


def test_schemas_tasks():
    """Test the creation of a new model from a parquet file."""
    schema_files = ["clients.json", "projects.json", "tasks.json"]

    origin_dir = Path(__file__).parent / "data" / "schemas"

    for schema_file in schema_files:
        schema = Schema.load_file(str(origin_dir / schema_file))
        assert "name" in schema
        assert "attributes" in schema


@pytest.mark.parametrize(
    "schema_file, extra_config",
    [
        ("clients.json", {}),
        ("projects.json", {"client_id": {"depends-on": "Client.id"}}),
        (
            "tasks.json",
            {
                "client_id": {"depends-on": "Project.client_id"},
                "project_id": {"depends-on": "Project.id"},
            },
        ),
    ],
)
def test_schemas_tasks_with_extra_config(schema_file, extra_config):
    """Test the creation of a new model from a parquet file."""
    origin_dir = Path(__file__).parent / "data" / "schemas"
    config_extra_file = origin_dir / "__extra-config__.json"

    file_path = str(origin_dir / schema_file)
    schema = Schema.load_file(file_path, config_extra_file)

    for k_attr, v_attr in extra_config.items():
        for k_prop, v_prop in v_attr.items():
            assert k_prop in schema["attributes"][k_attr]
            assert v_prop == schema["attributes"][k_attr][k_prop]
