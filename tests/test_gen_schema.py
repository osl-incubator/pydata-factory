"""Tests for `pydata_factory` package."""
from pathlib import Path

import pytest

from pydata_factory.schema import create_schema


@pytest.mark.parametrize("filename", ["fb2021.parquet", "msft2021.parquet"])
def test_create_schema(filename):
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "original" / filename
    target_dir = Path(__file__).parent / "data" / "schemas"
    create_schema(str(origin), str(target_dir))
