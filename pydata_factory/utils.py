import os

import pandas as pd


def get_class_name(name: str, namespace: str = ""):
    if name.endswith("ies"):
        name = name[:-3] + "y"
    elif name.endswith("s"):
        name = name[:-1]

    return name.title().replace("_", "") + namespace.title()


def get_class_name_from_path(data_path: str):
    name = data_path.split(os.sep)[-1].replace(".parquet", "")

    return get_class_name(name)


def get_attr_name(attr_name: str):
    return attr_name.replace(" ", "_").lower()


def normalize_datetime(value):
    if pd.isnull(value):
        return None

    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()

    # note: this logic needs to be improved
    datetime_format = (
        "%Y-%m-%d"
        if not (value.hour or value.minute or value.second)
        else "%Y-%m-%d %H:%M:%S"
    )
    return value.strftime(datetime_format)
