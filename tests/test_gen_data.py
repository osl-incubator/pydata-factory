"""Tests for `pydata_factory` package."""
from pathlib import Path

import pandas as pd

from pydata_factory.gen_data import gen_data


def test_gen_data():
    """Test the creation of a new model from a parquet file."""
    origin = Path(__file__).parent / "data" / "fb2021.parquet"
    target = "/tmp/fb2021.parquet"

    gen_data(str(origin), target, rows=600)

    df = pd.read_parquet(target)

    assert not df.empty
