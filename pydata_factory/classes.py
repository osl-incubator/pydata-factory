"""
Module for class factory generation.
"""
import datetime


class Model:
    ...


class GenModel:
    default_values = {
        "str": '""',
        "int": "0",
        "float": "0.0",
        "datetime": "datetime.datetime.now()",
    }

    ATTRIBUTE_TMPL = "    {name}: {type} = {value}"

    CLASS_TMPL = (
        "@dataclass\n" "class {name}Model(Model):\n" "{attributes}\n" "\n"
    )

    @staticmethod
    def generate(schema: dict):
        """
        Create a class model for the dataset path.
        """
        name = schema["name"]
        class_name = name

        attributes = []
        for k_attr, v_attr in schema["attributes"].items():
            t = schema["attributes"][k_attr]["dtype"]
            v = GenModel.default_values[t]

            if k_attr == "id":
                t = "int"

            if k_attr.endswith("_id") and v_attr.get("depends-on"):
                dep = v_attr.get("depends-on").split(".")[0]
                t = f"{dep}Model"
                v = "None"

            attributes.append(
                GenModel.ATTRIBUTE_TMPL.format(name=k_attr, type=t, value=v)
            )

        return GenModel.CLASS_TMPL.format(
            name=class_name, attributes="\n".join(attributes)
        )


class GenFactory:
    ATTRIBUTE_TMPL = "    {name} = {value}"

    CLASS_TMPL = (
        "class {name}Factory(factory.Factory):\n\n"
        "    class Meta:\n"
        "        model: Model = {model_class}\n\n"
        "{attributes}\n\n"
    )

    @staticmethod
    def generate(schema: dict, module: str, context_schemas: dict) -> str:
        """
        Create a class factory for the dataset path.
        """
        name = schema["name"]
        class_name = name
        model_class = f"{name}Model"

        attributes = []
        for k_attr, v_attr in schema["attributes"].items():
            if "__" in k_attr:
                # note: skip columns with "__" in the name to avoid conflict
                # with factory
                continue

            col = schema["attributes"][k_attr]
            t = col["dtype"]

            v = "None"

            if k_attr == "id":
                v_min = int(col.get("min", 1))
                v = f"factory.Sequence(lambda n: n + {v_min})"

            elif k_attr == "address":
                v = "factory.Faker('address')"

            elif k_attr == "name":
                v = f"factory.Sequence(lambda n: f'{class_name}' + str(n))"

            elif k_attr == "first_name":
                v = "factory.Sequence(lambda n: f'FirstName{n}')"

            elif k_attr == "last_name":
                v = "factory.Sequence(lambda n: f'LastName{n}')"

            elif k_attr.endswith("_id") and v_attr.get("depends-on"):
                t = "factory.Factory"

                if v_attr.get("__factory__"):
                    v = v_attr.get("__factory__").format(module=module)
                else:
                    dep_class, dep_attr = v_attr.get("depends-on").split(".")
                    dep_attr_ref = context_schemas[dep_class]["attributes"][
                        dep_attr
                    ]

                    id_min = dep_attr_ref.get("min", 1)
                    id_max = dep_attr_ref.get("max", 9999)

                    v = (
                        f"factory.SubFactory('{module}.{dep_class}Factory', "
                        "id=factory.LazyAttribute(lambda obj: "
                        f"random.randint({id_min}, {id_max})))"
                    )

            elif t == "int":
                v_min = col.get("min", 0)
                v_max = col.get("max", 9999)

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

            elif t in ["date", "datetime"]:
                v_min_str = str(col.get("min") or "")
                v_max_str = str(col.get("max") or "")

                dt_tmpl = "datetime.date({}, {}, {})"

                dttm_tmpl = (
                    "datetime.datetime({}, {}, {}, "
                    "tzinfo=datetime.timezone.utc)"
                )

                if not v_min_str or not v_max_str:
                    _now = datetime.datetime.now()
                    dt_str = dt_tmpl.format(_now.year, _now.month, _now.day)
                    v = f"FuzzyDate({dt_str})"
                else:
                    # note: add support for FuzzyDateTime as well
                    _v_min = [int(v) for v in v_min_str[:10].split("-")]
                    _v_max = [int(v) for v in v_max_str[:10].split("-")]

                    start_date_str = dttm_tmpl.format(
                        _v_min[0], _v_min[1], _v_min[2]
                    )
                    end_date_str = dttm_tmpl.format(
                        _v_max[0], _v_max[1], _v_max[2]
                    )
                    fuzzy_class = "FuzzyDate"

                    v = f"{fuzzy_class}({start_date_str}, {end_date_str})"

            attributes.append(
                GenFactory.ATTRIBUTE_TMPL.format(name=k_attr, value=v)
            )

        return GenFactory.CLASS_TMPL.format(
            name=class_name,
            attributes="\n".join(attributes),
            model_class=model_class,
        )
