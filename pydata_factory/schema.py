"""
Create datasets with fake data for testing.
"""
import json
import os
from typing import Optional

import pandas as pd
import sqlalchemy as sqla

from pydata_factory.config import MAPS_FROM_PANDAS_TYPES, MAPS_TO_PANDAS_TYPES
from pydata_factory.utils import (
    get_attr_name,
    get_class_name,
    normalize_datetime,
)


def cast_or_null(value, func):
    return None if pd.isnull(value) else func(value)


class Schema:
    @staticmethod
    def get_schema(df, physical_name: str, namespace: str = ""):
        name = get_class_name(physical_name, namespace)
        schema = {
            "name": name,
            "physical-name": physical_name,
            "namespace": namespace,
        }
        schema["attributes"] = {}

        attrs = schema["attributes"]
        for k in df.columns:
            k_new = get_attr_name(k)
            attrs[k_new] = {"physical-name": k}

            attrs[k_new]["physical-dtype"] = str(df[k].dtype)
            dtype = MAPS_FROM_PANDAS_TYPES[attrs[k_new]["physical-dtype"]]
            attrs[k_new]["dtype"] = dtype

            if k_new.endswith("_id"):
                # NOTE: it can be override using a config-extra file
                dep_name = get_class_name(k_new[:-3], namespace)
                attrs[k_new]["depends-on"] = f"{dep_name}.id"

            if dtype in ['int', 'float']:
                f = int if dtype.startswith("int") else float
                attrs[k_new]["min"] = cast_or_null(df[k].min(), f)
                attrs[k_new]["max"] = cast_or_null(df[k].max(), f)
                attrs[k_new]["mean"] = cast_or_null(df[k].mean(), f)
                attrs[k_new]["std"] = cast_or_null(df[k].std(), f)
                attrs[k_new]["count"] = cast_or_null(df[k].count(), int)
            elif dtype in ["date", "datetime"]:
                attrs[k_new]["min"] = normalize_datetime(df[k].min())
                attrs[k_new]["max"] = normalize_datetime(df[k].max())
            elif dtype == "str":
                uniques = df[k].unique()
                threshold = df.shape[0] / 5
                if 0 > len(uniques) <= threshold:
                    attrs[k_new]["categories"] = uniques.tolist()

            for k, v in list(attrs[k_new].items()):
                if pd.isnull(v):
                    attrs[k_new][k] = None
        return schema

    @staticmethod
    def load_file(path: str, config_extra_file: Optional[str] = None):

        config_extra = {}

        if config_extra_file:
            with open(config_extra_file, "r") as f:
                content = f.read()
                config_extra = json.loads(content)

        with open(path, "r") as f:
            content = f.read()
            schema = json.loads(content)

        schema_name = schema["name"]

        if config_extra and schema_name in config_extra:
            for k_attr, v_attr in config_extra[schema_name][
                "attributes"
            ].items():
                schema["attributes"][k_attr].update(v_attr)

        return schema

    @staticmethod
    def to_dataframe(schema):
        df = pd.DataFrame({}, columns=schema["attributes"].keys())
        dtypes = {
            k: MAPS_TO_PANDAS_TYPES[schema["attributes"][k]["dtype"]]
            for k in df.keys()
        }
        return df.astype(dtypes)

    @staticmethod
    def from_parquet(
        origin: str, target_dir: str, namespace: str = ""
    ) -> dict:
        """
        Create a empty file just with the dataset schema.
        """
        os.makedirs(target_dir, exist_ok=True)

        filename = origin.split(os.sep)[-1].split('.')[0]

        target_file = f"{target_dir}/{filename}.json"

        physical_name = filename

        df = pd.read_parquet(origin)
        schema = Schema.get_schema(df, physical_name, namespace)

        with open(target_file, "w") as f:
            json.dump(schema, fp=f, indent=2)

        return schema

    @staticmethod
    def from_sql(
        engine: sqla.engine.base.Engine,
        table_name: str,
        target_dir: str,
        namespace: str = "",
    ) -> dict:
        """
        Create a empty file just with the dataset schema.
        """
        os.makedirs(target_dir, exist_ok=True)

        target_file = f"{target_dir}/{table_name}.json"

        physical_name = table_name

        df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
        schema = Schema.get_schema(df, physical_name, namespace)

        with open(target_file, "w") as f:
            json.dump(schema, fp=f, indent=2)

        return schema

    @staticmethod
    def get_map_physical_attributes(schema: dict) -> dict:
        map_attr = {}
        for k_attr, v_attr in schema["attributes"].items():
            map_attr[k_attr] = v_attr.get("physical-name", k_attr)
        return map_attr

    @staticmethod
    def get_qualified_name(schema: dict) -> str:
        physical_name = schema["physical-name"]
        namespace = schema.get("namespace", "")

        return (
            physical_name if not namespace else f"{namespace}.{physical_name}"
        )

    @staticmethod
    def get_priorities(config_file: str) -> list:
        with open(config_file, "r") as f:
            content = f.read()
            config = json.loads(content)

        return config.get("__config__", {}).get("priorities", [])
