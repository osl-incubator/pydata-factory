import datetime
import importlib
import os
import sys
from typing import Dict

import factory
import factory.random
import pandas as pd
from faker import Faker

from pydata_factory.classes import GenFactory, GenModel, Model
from pydata_factory.schema import Schema

Faker.seed(42)
factory.random.reseed_random(42)


class GenData:
    @staticmethod
    def generate(
        schemas: dict, rows: dict = {}, priorities: list = []
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate fake data from a dataset file.
        """

        tmp_dir = "/tmp/pydata_factory_classes"
        os.makedirs(tmp_dir, exist_ok=True)
        script_name = datetime.datetime.now().strftime("pydf_%Y%m%d_%H%M%S_%f")
        script_path = f"{tmp_dir}/{script_name}.py"

        header = (
            "from __future__ import annotations\n"
            "import datetime\n"
            "from dataclasses import dataclass\n"
            "import random\n\n"
            "import factory\n"
            "import factory.random\n"
            "from factory.fuzzy import FuzzyDate, FuzzyDateTime\n"
            "from faker import Faker\n\n"
            "from pydata_factory.classes import Model\n\n"
            "Faker.seed(42)\n"
            "\n"
            "factory.random.reseed_random(42)\n\n\n"
        )

        model_script = ""
        factory_script = ""

        for k_schema, schema in schemas.items():
            name = schema["name"]
            namespace = schema.get("namespace", "")

            model_script += GenModel.generate(schema) + "\n"
            factory_script += (
                GenFactory.generate(schema, script_name, schemas) + "\n"
            )

            if name not in rows or not rows[name]:
                rows[name] = 1
                for k, v in schema["attributes"].items():
                    if "count" not in v:
                        continue
                    rows[name] = int(max(rows[name], v["count"]))

        script = model_script + factory_script

        with open(f"{tmp_dir}/__init__.py", "w") as f:
            f.write("")

        with open(script_path, "w") as f:
            f.write(header + script)

        sys.path.insert(0, tmp_dir)
        lib_tmp = importlib.import_module(script_name)

        dfs = {}

        if not priorities:
            priorities = list(schemas.keys())

        for k_schema in priorities:
            schema = schemas[k_schema]
            name = schema["name"]
            original_name = schema["original-name"]
            namespace = schema.get("namespace", "")
            class_name = name
            results = []

            df = Schema.to_dataframe(schema)

            for i in range(rows[name]):
                obj = getattr(lib_tmp, f"{class_name}Factory")()
                data = obj.__dict__
                data = {
                    k: v.id if isinstance(v, Model) else v  # type: ignore
                    for k, v in data.items()
                }
                results.append(data)

            qualified_name = (
                original_name
                if not namespace
                else f"{namespace}.{original_name}"
            )
            dfs[qualified_name] = pd.concat([df, pd.DataFrame(results)])

        return dfs
