from pathlib import Path

import polars as pl

f_pqt = Path(__file__).parent.parent.resolve() / "parquet"


for filename in [
    "sample-small-2.pqt",
    "sample-big-2.pqt",
]:

    path = f_pqt / filename

    print(f"read file {path.name}")
    schema = pl.read_parquet_schema(path)
    print(schema)
    df = pl.read_parquet(path)
    print(df)
