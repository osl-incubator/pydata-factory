"""
Create datasets with fake data for testing.
"""
import json
import os

import pandas as pd

from pydata_factory.utils import get_attr_name, get_class_name


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
            attrs[k_new]["min"] = f(df[k].min())
            attrs[k_new]["max"] = f(df[k].max())
            attrs[k_new]["mean"] = f(df[k].mean())
            attrs[k_new]["std"] = f(df[k].std())
            attrs[k_new]["count"] = f(df[k].count())
        elif dtype.startswith("date"):
            attrs[k_new]["min"] = df[k].min()
            attrs[k_new]["max"] = df[k].max()
        elif dtype.startswith("object"):
            uniques = df[k].unique()
            threshold = df.shape[0] / 5
            if len(uniques) <= threshold:
                attrs[k_new]["categories"] = uniques.tolist()
    return schema


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
