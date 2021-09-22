"""
Module for class factory generation.
"""
import pandas as pd

from pydata_factory.utils import get_class_name

ATTRIBUTE_FACTORY_TMPL = "    {name} = {value}"

CLASS_FACTORY_TMPL = """\
@dataclass
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


def create_factory(data_path: str):
    """
    Create a class factory for the dataset path.

    Parameters
    ----------
    data_path: str
    """
    name = get_class_name(data_path)

    name_formatted = name.title().replace("_", "")

    model_class = f"{name_formatted}Model"

    df = pd.read_parquet(data_path)

    attributes = []
    for c in df.columns:
        t = maps_factory_types[str(df[c].dtype)]

        v = "None"

        if c == "id":
            v = "factory.Sequence(lambda n: n + 1)"
        elif c == "address":
            v = "factory.Faker('address')"
        elif c == "name":
            v = "factory.Faker('name')"
        elif t == "datetime":
            v = "factory.LazyAttribute(lambda o: datetime.now())"
        elif c.endswith("_id"):
            t = c.replace("_id", "").title().replace("_", "")
            t += "Factory"
            v = "None"
        elif t == "int":
            v_min = df[c].min()
            v_max = df[c].max()

            if v_min == v_max:
                v = str(v_min)
            else:
                v = (
                    "factory.LazyAttribute(lambda o: "
                    f"random.randint({v_min}, {v_max}))"
                )
        elif t == "float":
            v_min = int(df[c].min())
            v_max = int(df[c].max())

            if v_min == v_max:
                v = str(v_min)
            else:
                v = (
                    "factory.LazyAttribute(lambda o: 1.0 * "
                    f"random.randint({v_min}, {v_max}))"
                )

        elif t == "str":
            threshold = df.shape[0] / 10
            uniques = df[c].unique()
            if len(uniques) < threshold:
                options = tuple([(v, v) for v in uniques])
                v = (
                    "factory.Iterator({options}, " "getter=lambda c: c[0])"
                ).format(options=options)
            else:
                v = ""

        c = c.replace(" ", "_").lower()

        attributes.append(ATTRIBUTE_FACTORY_TMPL.format(name=c, value=v))

    return CLASS_FACTORY_TMPL.format(
        name=name_formatted,
        attributes="\n".join(attributes),
        model_class=model_class,
    )
