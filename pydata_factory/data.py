import datetime
import importlib
import os
import sys
from typing import Dict

import factory
import factory.random
import pandas as pd
from faker import Faker

from pydata_factory.classes import GenFactory, GenModel
from pydata_factory.schema import Schema
from pydata_factory.utils import get_class_name

Faker.seed(42)
factory.random.reseed_random(42)


class GenData:
    @staticmethod
    def generate(
        schemas: list,
        rows: dict = {},
    ) -> Dict[str, pd.DataFrame]:
        """
        Generate fake data from a dataset file.
        """

        header = (
            "from __future__ import annotations\n"
            "import datetime\n"
            "from dataclasses import dataclass\n"
            "import random\n\n"
            "import factory\n"
            "import factory.random\n"
            "from factory.fuzzy import FuzzyDate, FuzzyDateTime\n"
            "from faker import Faker\n\n"
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

            model_script += GenModel.generate(schema) + "\n"
            factory_script += GenFactory.generate(schema) + "\n"

            if name not in rows or not rows[name]:
                rows[name] = 1
                for k, v in schema["attributes"].items():
                    if "count" not in v:
                        continue
                    rows[name] = int(max(rows[name], v["count"]))

        script = model_script + factory_script

        tmp_dir = "/tmp/pydata_factory_classes"
        os.makedirs(tmp_dir, exist_ok=True)
        script_name = datetime.datetime.now().strftime("pydf_%Y%m%d_%H%M%S_%f")
        script_path = f"{tmp_dir}/{script_name}.py"

        with open(f"{tmp_dir}/__init__.py", "w") as f:
            f.write("")

        with open(script_path, "w") as f:
            f.write(header + script)

        sys.path.insert(0, tmp_dir)
        lib_tmp = importlib.import_module(script_name)

        dfs = {}

        for schema in schemas:
            results = []
            name = schema["name"]
            original_name = schema["original-name"]
            class_name = get_class_name(name)

            df = Schema.to_dataframe(schema)

            for i in range(rows[name]):
                obj = getattr(lib_tmp, f"{class_name}Factory")()
                results.append(obj.__dict__)

            dfs[original_name] = pd.concat([df, pd.DataFrame(results)])

        return dfs
