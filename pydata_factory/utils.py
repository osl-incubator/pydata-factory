import os


def get_class_name(data_path: str):
    name = data_path.split(os.sep)[-1].replace(".parquet", "")

    if name.endswith("ies"):
        name = name[:-3] + "y"
    elif name.endswith("s"):
        name = name[:-1]

    return name


def get_attr_name(attr_name: str):
    return attr_name.replace(" ", "_").lower()
