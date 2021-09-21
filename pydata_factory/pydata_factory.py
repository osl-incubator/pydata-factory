"""
Create datasets with fake data for testing.
"""
import os

import pandas as pd


def create_schema_file(origin: str, target: str):
    """
    Create a empty file just with the dataset schema.

    Parameters
    ----------
    path: str
    """
    df = pd.read_parquet(origin)
    target_dir = os.sep.join(target.split(os.sep)[:-1])
    os.makedirs(target_dir, exist_ok=True)
    df[0:0].to_parquet(target)


def create_model(origin: str, output_code: str):
    df = pd.read_parquet(origin)
    assert df is not None
