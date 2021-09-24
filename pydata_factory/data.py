import importlib
import os
import random  # noqa: F401
import sys
from dataclasses import dataclass  # noqa: F401
from datetime import datetime  # noqa: F401
from typing import Dict
from uuid import uuid4

import factory
import factory.random
import pandas as pd
from faker import Faker

from pydata_factory.class_factory import create_factory
from pydata_factory.class_model import create_model
from pydata_factory.schema import create_data_frame_from_schema
from pydata_factory.utils import get_class_name

Faker.seed(42)
factory.random.reseed_random(42)


def gen_data_from_schema(schema: dict, rows: int = None) -> pd.DataFrame:
    """
    Generate fake data from a dataset file.
    """

    df = create_data_frame_from_schema(schema)

    name = schema["name"]

    model_script = create_model(schema)
    factory_script = create_factory(schema)

    script = model_script + factory_script

    script_import = (
        "from datetime import datetime\n"
        "from dataclasses import dataclass\n"
        "import factory\n"
        "import random\n"
        "import factory.random\n"
        "from faker import Faker\n"
        "Faker.seed(42)\n"
        "\n"
        "factory.random.reseed_random(42)\n\n\n"
        "class Model:\n"
        "    ...\n\n\n"
    )

    tmp_dir = "/tmp/pydata_factory_classes"
    os.makedirs(tmp_dir, exist_ok=True)
    script_name = f"a{uuid4().hex}"
    script_path = f"{tmp_dir}/{script_name}.py"

    with open(f"{tmp_dir}/__init__.py", "w") as f:
        f.write("from .{script_name} import *")

    with open(script_path, "w") as f:
        f.write(script_import + script)

    sys.path.append(tmp_dir)
    lib_tmp = importlib.import_module(script_name)

    if not rows:
        rows = 1
        for k, v in schema["attributes"].items():
            if "count" not in v:
                continue
            rows = int(max(rows, v["count"]))

    results = []

    for i in range(rows):
        obj = getattr(lib_tmp, f"{name.title()}Factory")()
        results.append(obj.__dict__)

    return pd.concat([df, pd.DataFrame(results)])


def gen_data_from_schemas(
    schemas: list,
    rows: dict = {},
    use_foreign_key: bool = False,
    exclude_foreign_key: list = [],
) -> Dict[str, pd.DataFrame]:
    """
    Generate fake data from a dataset file.
    """

    header = (
        "from __future__ import annotations\n"
        "from datetime import datetime\n"
        "from dataclasses import dataclass\n"
        "import factory\n"
        "import random\n"
        "import factory.random\n"
        "from faker import Faker\n"
        "Faker.seed(42)\n"
        "\n"
        "factory.random.reseed_random(42)\n\n\n"
        "class Model:\n"
        "    ...\n\n\n"
    )

    model_script = ""
    factory_script = ""

    for schema in schemas:
        name = schema["name"]

        model_script += (
            create_model(
                schema,
                use_foreign_key=use_foreign_key,
                exclude_foreign_key=exclude_foreign_key,
            )
            + "\n"
        )
        factory_script += (
            create_factory(
                schema,
                use_foreign_key=use_foreign_key,
                exclude_foreign_key=exclude_foreign_key,
            )
            + "\n"
        )

        if name not in rows or not rows[name]:
            rows[name] = 1
            for k, v in schema["attributes"].items():
                if "count" not in v:
                    continue
                rows[name] = int(max(rows[name], v["count"]))

    script = model_script + factory_script

    tmp_dir = "/tmp/pydata_factory_classes"
    os.makedirs(tmp_dir, exist_ok=True)
    script_name = f"a{uuid4().hex}"
    script_path = f"{tmp_dir}/{script_name}.py"

    with open(f"{tmp_dir}/__init__.py", "w") as f:
        f.write("from .{script_name} import *")

    with open(script_path, "w") as f:
        f.write(header + script)

    sys.path.append(tmp_dir)
    lib_tmp = importlib.import_module(script_name)

    dfs = {}

    for schema in schemas:
        results = []
        name = schema["name"]
        class_name = get_class_name(name)

        df = create_data_frame_from_schema(schema)

        for i in range(rows[name]):
            obj = getattr(lib_tmp, f"{class_name}Factory")()
            results.append(obj.__dict__)

        dfs[name] = pd.concat([df, pd.DataFrame(results)])

    return dfs
