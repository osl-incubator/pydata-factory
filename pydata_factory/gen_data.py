import random  # noqa: F401
from dataclasses import dataclass  # noqa: F401
from datetime import datetime  # noqa: F401

import factory
import factory.random
import pandas as pd
from faker import Faker

from pydata_factory.gen_factory import create_factory
from pydata_factory.gen_model import create_model
from pydata_factory.utils import get_attr_name, get_class_name

Faker.seed(42)
factory.random.reseed_random(42)


class Model:
    ...


@dataclass
class Fb2021Model(Model):
    high: float = 0.0
    low: float = 0.0
    open: float = 0.0
    close: float = 0.0
    volume: int = 0
    adj_close: float = 0.0


def gen_data(origin: str, target: str, rows: int = None):
    """
    Generate fake data from a dataset file.
    """

    df = pd.read_parquet(origin)

    name = get_class_name(origin).title()

    model_script = create_model(origin)
    factory_script = create_factory(origin)

    exec(model_script)
    exec(factory_script)

    if rows is None:
        rows = df.shape[0]

    results = []

    for i in range(rows):
        obj = locals()[f"{name}Factory"]()
        results.append(obj.__dict__)

    df = pd.read_parquet(origin)[0:0]
    df.rename(columns={c: get_attr_name(c) for c in df.columns}, inplace=True)
    df = pd.concat([df, pd.DataFrame(results)])
    df.to_parquet(target)
