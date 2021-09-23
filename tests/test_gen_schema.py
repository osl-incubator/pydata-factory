"""Tests for `pydata_factory` package."""
from pathlib import Path

from pydata_factory.schema import create_schema


def test_create_schema():
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "original" / "fb2021.parquet"
    target_dir = Path(__file__).parent / "data" / "schemas"
    create_schema(str(origin), str(target_dir))
