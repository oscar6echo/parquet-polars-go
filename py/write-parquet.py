import datetime as dt
from pathlib import Path
from timeit import default_timer as timer

import polars as pl
import pyarrow.parquet as pq

f_pqt = Path(__file__).parent.parent.resolve() / "parquet"


def build_df(N=1, selection=[i for i in range(10)]):
    """"""
    t0 = timer()
    dic_series = {}
    now = dt.datetime.now()

    dic_series[0] = pl.Series("idx", [e for e in range(2 * N)], dtype=pl.Int32)
    dic_series[1] = pl.Series("name", ["azerty", "qwerty"] * N, dtype=pl.Utf8)
    dic_series[2] = pl.Series("age", [22, 23] * N, dtype=pl.Int64)
    dic_series[3] = pl.Series("sex", [True, False] * N, dtype=pl.Boolean)
    dic_series[4] = pl.Series("weight", [51.2, 65.3] * N, dtype=pl.Float64)
    dic_series[5] = pl.Series("atime", [now, now] * N, dtype=pl.Datetime)
    dic_series[6] = pl.Series("adate", [now, now] * N, dtype=pl.Date)
    dic_series[7] = pl.Series(
        "list_int", [[10, 20], [11, 22]] * N, dtype=pl.List(pl.Int64)
    )
    dic_series[8] = pl.Series(
        "list_float", [[100.5, 200.1], [115.8, 225.4]] * N, dtype=pl.List(pl.Float64)
    )
    dic_series[9] = pl.Series(
        "list_struct",
        [{"name": "London", "score": 15}, {"name": "Rome", "score": 17}] * N,
        dtype=pl.List(pl.Struct),
    )

    df = pl.DataFrame([dic_series[i] for i in selection])
    t1 = timer()
    print("-" * 10)
    print("columns:")
    print(df.dtypes)
    print("df:")
    print(df)
    print(f"df built in {t1-t0:.2f} s\n")
    return df


def save_df(df, filename):
    """"""
    t0 = timer()
    path = f_pqt / filename
    # compression: snappy, gzip, zstd
    df.write_parquet(
        path,
        compression="zstd",
        use_pyarrow=True,
        # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html
        pyarrow_options={"use_compliant_nested_type": True},
    )
    t1 = timer()
    print(f"df saved in {t1-t0:.2f} s: {path.name}\n")


def show_parquet_schema(filename, show_data=False):
    """"""
    print("-" * 10)
    print(f"schema {filename}:")
    path = f_pqt / filename
    c = pq.ParquetFile(path)
    print(c.schema)
    print()
    if show_data:
        print(f"data:")
        print(c.read())
        print()


for N, filename, selection in [
    (1, "sample-small.pqt", [i for i in range(10)]),
    (1_000_000, "sample-big.pqt", [i for i in range(10)]),
]:
    print("*" * 20, f"N={N} filename={filename}")
    df = build_df(N, selection)
    save_df(df, filename)
    show_data = N < 10
    show_parquet_schema(filename, show_data=show_data)
