"""
Module for class model generation.
"""
import os

import pandas as pd

ATTRIBUTE_TMPL = "    {name}: {type} = {value}"

CLASS_TMPL = """\
@dataclass
class {name}Model(Model):
{attributes}

"""

maps_types = {
    "object": "str",
    "datetime64[ns, UTC]": "datetime",
    "datetime64[ns]": "datetime",
    "int64": "int",
    "int32": "int",
    "float64": "float",
    "float32": "float",
}

default_values = {
    "str": '""',
    "int": "0",
    "float": "0.0",
    "datetime": "datetime.now()",
}


def create_model(data_path: str):
    """
    Create a class model for the dataset path.

    Parameters
    ----------
    data_path: str
    """
    name = data_path.split(os.sep)[-1].replace(".parquet", "")

    if name.endswith("ies"):
        name = name[:-3] + "y"
    elif name.endswith("s"):
        name = name[:-1]

    df = pd.read_parquet(data_path)

    attributes = []
    for c in df.columns:
        t = maps_types[str(df[c].dtype)]
        v = default_values[t]

        c = c.replace(" ", "_").lower()

        if c == "id":
            t = "int"

        if c.endswith("_id"):
            t = c.replace("_id", "").title().replace("_", "")
            t += f"{name.title()}Model"
            v = "None"

        attributes.append(ATTRIBUTE_TMPL.format(name=c, type=t, value=v))

    return CLASS_TMPL.format(
        name=name.title().replace("_", ""), attributes="\n".join(attributes)
    )
