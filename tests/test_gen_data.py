"""Tests for `pydata_factory` package."""
from pathlib import Path

import pandas as pd

from pydata_factory.data import gen_data
from pydata_factory.schema import load_schema


def test_gen_data():
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "schemas" / "fb2021.json"
    target = Path(__file__).parent / "data" / "synthetic" / "fb2021.parquet"

    schema = load_schema(origin)
    gen_data(schema, target)

    df = pd.read_parquet(target)

    assert not df.empty
