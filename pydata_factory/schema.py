"""
Create datasets with fake data for testing.
"""
import json
import os

import pandas as pd

from pydata_factory.utils import (
    get_attr_name,
    get_class_name,
    normalize_datetime,
)


def cast_or_null(value, func):
    return None if pd.isnull(value) else func(value)


def get_schema(df, name):
    schema = {"name": name}
    schema["attributes"] = {}

    attrs = schema["attributes"]
    for k in df.columns:
        k_new = get_attr_name(k)
        attrs[k_new] = {}

        dtype = str(df[k].dtype)
        attrs[k_new]["dtype"] = dtype

        if dtype.startswith("int") or dtype.startswith("float"):
            f = int if dtype.startswith("int") else float
            attrs[k_new]["min"] = cast_or_null(df[k].min(), f)
            attrs[k_new]["max"] = cast_or_null(df[k].max(), f)
            attrs[k_new]["mean"] = cast_or_null(df[k].mean(), f)
            attrs[k_new]["std"] = cast_or_null(df[k].std(), f)
            attrs[k_new]["count"] = cast_or_null(df[k].count(), f)
        elif dtype.startswith("date"):
            attrs[k_new]["min"] = normalize_datetime(df[k].min())
            attrs[k_new]["max"] = normalize_datetime(df[k].max())
        elif dtype.startswith("object"):
            uniques = df[k].unique()
            threshold = df.shape[0] / 5
            if 0 > len(uniques) <= threshold:
                attrs[k_new]["categories"] = uniques.tolist()

        for k, v in list(attrs[k_new].items()):
            if pd.isnull(v):
                attrs[k_new][k] = None
    return schema


def load_schema(path: str):
    with open(path, "r") as f:
        content = f.read()
        return json.loads(content)


def create_data_frame_from_schema(schema):
    df = pd.DataFrame({}, columns=schema["attributes"].keys())
    dtypes = {k: schema["attributes"][k]["dtype"] for k in df.keys()}
    return df.astype(dtypes)


def create_schema(origin: str, target_dir: str, name=None):
    """
    Create a empty file just with the dataset schema.
    """
    os.makedirs(target_dir, exist_ok=True)

    filename = origin.split(os.sep)[-1].split('.')[0]

    target_file = f"{target_dir}/{filename}.json"

    if name is None:
        name = get_class_name(filename)

    df = pd.read_parquet(origin)
    schema = get_schema(df, name)

    with open(target_file, "w") as f:
        json.dump(schema, fp=f)
