"""
Module for class model generation.
"""
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


def create_model(schema: dict):
    """
    Create a class model for the dataset path.
    """
    name = schema["name"]

    attributes = []
    for c in schema["attributes"]:
        t = maps_types[str(schema["attributes"][c]["dtype"])]
        v = default_values[t]

        c = c.replace(" ", "_").lower()

        if c == "id":
            t = "int"

        if c.endswith("_id"):
            t = c.replace("_id", "").title().replace("_", "")
            t += f"{name}Model"
            v = "None"

        attributes.append(ATTRIBUTE_TMPL.format(name=c, type=t, value=v))

    return CLASS_TMPL.format(name=name, attributes="\n".join(attributes))
