"""
Module for class model generation.
"""
from pydata_factory.utils import get_class_name

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


def create_model(
    schema: dict, use_foreign_key: bool = False, exclude_foreign_key: list = []
):
    """
    Create a class model for the dataset path.
    """
    name = schema["name"]
    class_name = get_class_name(name)

    attributes = []
    for c in schema["attributes"]:
        t = maps_types[str(schema["attributes"][c]["dtype"])]
        v = default_values[t]

        c = c.replace(" ", "_").lower().replace("__", "_0000_")

        if c == "id":
            t = "int"

        if (
            use_foreign_key
            and c.endswith("_id")
            and c not in exclude_foreign_key
        ):
            t = get_class_name(c.replace("_id", ""))
            t += "Model"
            v = "None"

        attributes.append(ATTRIBUTE_TMPL.format(name=c, type=t, value=v))

    return CLASS_TMPL.format(name=class_name, attributes="\n".join(attributes))
