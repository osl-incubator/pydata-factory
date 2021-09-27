MAPS_FROM_PANDAS_TYPES = {
    "object": "str",
    "datetime64[ns, UTC]": "datetime",
    "datetime64[ns]": "datetime",
    "int64": "int",
    "int32": "int",
    "float64": "float",
    "float32": "float",
}


MAPS_TO_PANDAS_TYPES = {
    "str": "object",
    "datetime": "datetime64[ns]",
    "int": "int64",
    "float": "float64",
}
