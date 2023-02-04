from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

f_pqt = Path(__file__).parent.parent.resolve() / "parquet"


for filename in [
    "sample-small-a.pqt",
    # "sample-small-a-2.pqt",
    "sample-small-a-3.pqt",
    "sample-big-a.pqt",
    # "sample-big-a-2.pqt",
    "sample-big-a-3.pqt",
    # "sample-small-b.pqt",
    # "sample-small-b-2.pqt",
    # "sample-big-b.pqt",
    # "sample-big-b-2.pqt",
]:

    path = f_pqt / filename

    print(f"read file {path.name}")
    schema = pl.read_parquet_schema(path)
    print(schema)
    df = pl.read_parquet(path)

    # safer
    df = df.with_column(pl.col("atime").cast(pl.Datetime).keep_name())
    df = df.with_column(pl.col("adate").cast(pl.Date).keep_name())

    n = 6
    cols1 = df.columns[:n]
    cols2 = df.columns[n:]
    print(df.select(cols1))
    print(df.select(cols2))

    c = pq.ParquetFile(path)
    print(c.schema)
