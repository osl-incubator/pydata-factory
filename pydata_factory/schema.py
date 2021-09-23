"""
Create datasets with fake data for testing.
"""
import json
import os

import pandas as pd

from pydata_factory.utils import get_attr_name


def get_schema(df):
    schema = {}
    for k in df.columns:
        k_new = get_attr_name(k)
        schema[k_new] = {}

        dtype = str(df[k].dtype)
        schema[k_new]["dtype"] = dtype

        if dtype.startswith("int") or dtype.startswith("float"):
            f = int if dtype.startswith("int") else float
            schema[k_new]["min"] = f(df[k].min())
            schema[k_new]["max"] = f(df[k].max())
            schema[k_new]["mean"] = f(df[k].mean())
            schema[k_new]["std"] = f(df[k].std())
            schema[k_new]["count"] = f(df[k].count())
        elif dtype.startswith("date"):
            schema[k_new]["min"] = df[k].min()
            schema[k_new]["max"] = df[k].max()
        elif dtype.startswith("object"):
            uniques = df[k].unique()
            threshold = df.shape[0] / 5
            if len(uniques) <= threshold:
                schema[k_new]["categories"] = uniques.tolist()

    return schema


def data_frame_from_schema(schema):
    df = pd.DataFrame({}, columns=schema.keys())
    dtypes = {k: schema[k]["dtype"] for k in df.keys()}
    return df.astype(dtypes)


def create_schema(origin: str, target_dir: str):
    """
    Create a empty file just with the dataset schema.
    """
    os.makedirs(target_dir, exist_ok=True)

    target_file = f"{target_dir}/{origin.split(os.sep)[-1].split('.')[0]}.json"

    df = pd.read_parquet(origin)
    schema = get_schema(df)

    with open(target_file, "w") as f:
        json.dump(schema, fp=f)
