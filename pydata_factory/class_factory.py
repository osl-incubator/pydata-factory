"""
Module for class factory generation.
"""
from pydata_factory.utils import get_class_name

ATTRIBUTE_FACTORY_TMPL = "    {name} = {value}"

CLASS_FACTORY_TMPL = """\
class {name}Factory(factory.Factory):

    class Meta:
        model: Model = {model_class}

{attributes}

"""

maps_factory_types = {
    "object": "str",
    "datetime64[ns, UTC]": "datetime",
    "datetime64[ns]": "datetime",
    "int64": "int",
    "int32": "int",
    "float64": "float",
    "float32": "float",
}


def create_factory(
    schema: dict, use_foreign_key: bool = False, exclude_foreign_key: list = []
) -> str:
    """
    Create a class factory for the dataset path.
    """
    name = schema["name"]
    class_name = get_class_name(name)

    model_class = f"{get_class_name(name)}Model"

    attributes = []
    for c in schema["attributes"].keys():
        col = schema["attributes"][c]
        t = maps_factory_types[str(col["dtype"])]

        v = "None"

        if c == "id":
            v = "factory.Sequence(lambda n: n + 1)"
        elif c == "address":
            v = "factory.Faker('address')"
        elif c == "name":
            v = "factory.Faker('name')"
        elif t == "datetime":
            v = "factory.LazyAttribute(lambda o: datetime.now())"
        elif (
            use_foreign_key
            and c.endswith("_id")
            and c not in exclude_foreign_key
        ):
            t = c.replace("_id", "").title().replace("_", "")
            t += "Factory"
            v = "None"
        elif t == "int":
            v_min = col["min"]
            v_max = col["max"]

            if v_min == v_max:
                v = str(v_min)
            else:
                v = (
                    "factory.LazyAttribute(lambda o: "
                    f"random.randint({v_min}, {v_max}))"
                )
        elif t == "float":
            v_min = int(col["min"])
            v_max = int(col["max"])

            if v_min == v_max:
                v = str(v_min)
            else:
                v = (
                    "factory.LazyAttribute(lambda o: 1.0 * "
                    f"random.randint({v_min}, {v_max}))"
                )

        elif t == "str":

            if "categories" in col:
                options = tuple([(v, v) for v in col["categories"]])
                v = (
                    "factory.Iterator({options}, " "getter=lambda c: c[0])"
                ).format(options=options)
            else:
                v = '""'

        c = c.replace(" ", "_").lower().replace("__", "_0000_")

        attributes.append(ATTRIBUTE_FACTORY_TMPL.format(name=c, value=v))

    return CLASS_FACTORY_TMPL.format(
        name=class_name,
        attributes="\n".join(attributes),
        model_class=model_class,
    )
