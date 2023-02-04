# Python Polars <-> Parquet <-> Go

## Overview

This repo uses 3 libraries:

- [fraugster/parquet-go](https://github.com/fraugster/parquet-go)
- [xitongsys/parquet-go](https://github.com/xitongsys/parquet-go)
- [segmentio/parquet-go](https://github.com/segmentio/parquet-go)

Procedure:

- Create dataframes in [polars](https://www.pola.rs/) and save them as parquet files in folder [parquet](./parquet).
  - small (2 rows) and big(1m rows)
  - with simple types (straing, int, float, bool, datetimes) and with complex types (list[int, float, string, struct])
- Read theses files in go and write them to disk as parquet files
- Read the latter files in polars and make sure no information was lost in the round trip

Conclusion:

- **fraugster/parquet-go** is the only lib that produces polars compatible format for complex types, but is is the slowest and most verbose to achieve that
- **xitongsys/parquet-go** and **segmentio/parquet-go** are significantly faster but produce nested types that are not compatible with polars

It seems parquet format is quite permissive so different libs have generally little chance to be compatible beyond the most basic types.  
So it would be good if polars offered some flexibility in the parquet formatting of nested types to help compatibility with other ecosystems.

## User Guide

To run tests:

```sh
# from /py
# create and save sample polars dataframe
# with all sorts of column types incl. list and struct
# save as parquet file
python ./write_parquet.py
# output: /parquet/sample-(small|big)-(a|b).pqt

# from /go
# reaad parquet file and unmarshal to go struct
# save the data as parquet file again
go run ./main.go
# output: /parquet/sample-(small|big)-(2|3).pqt

# from /py
# read parquet file produced by go as polars dataframe
python ./read_parquet.py
```

## Write parquet from polars

Output:

```sh
❯ python write-parquet.py
******************** N=1 filename=sample-small-a.pqt
----------
columns:
[Int32, Utf8, Int64, Boolean, Float64, Datetime(tu='us', tz=None), Date]
df:
shape: (2, 7)
┌─────┬────────┬─────┬───────┬────────┬────────────────────────────┬────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      ┆ adate      │
│ --- ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        ┆ ---        │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               ┆ date       │
╞═════╪════════╪═════╪═══════╪════════╪════════════════════════════╪════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:05:26.269051 ┆ 2023-02-04 │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:05:26.269051 ┆ 2023-02-04 │
└─────┴────────┴─────┴───────┴────────┴────────────────────────────┴────────────┘
df built in 0.22 s

df saved in 0.00 s: sample-small-a.pqt

----------
schema sample-small-a.pqt:
<pyarrow._parquet.ParquetSchema object at 0x7f79a5b3da80>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
}


data:
pyarrow.Table
idx: int32
name: large_string
age: int64
sex: bool
weight: double
atime: timestamp[us]
adate: date32[day]
----
idx: [[0,1]]
name: [["azerty","qwerty"]]
age: [[22,23]]
sex: [[true,false]]
weight: [[51.2,65.3]]
atime: [[2023-02-04 19:05:26.269051,2023-02-04 19:05:26.269051]]
adate: [[2023-02-04,2023-02-04]]

******************** N=1 filename=sample-small-b.pqt
----------
columns:
[Int32, Utf8, Int64, Boolean, Float64, Datetime(tu='us', tz=None), Date, List(Int64), List(Float64), List(Utf8), Struct([Field('name': Utf8), Field('score': Int64)])]
df:
shape: (2, 11)
┌─────┬────────┬─────┬───────┬─────┬───────────┬────────────────┬────────────┬───────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ ... ┆ list_int  ┆ list_float     ┆ list_str   ┆ list_struct   │
│ --- ┆ ---    ┆ --- ┆ ---   ┆     ┆ ---       ┆ ---            ┆ ---        ┆ ---           │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆     ┆ list[i64] ┆ list[f64]      ┆ list[str]  ┆ struct[2]     │
╞═════╪════════╪═════╪═══════╪═════╪═══════════╪════════════════╪════════════╪═══════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
└─────┴────────┴─────┴───────┴─────┴───────────┴────────────────┴────────────┴───────────────┘
df built in 0.00 s

df saved in 0.00 s: sample-small-b.pqt

----------
schema sample-small-b.pqt:
<pyarrow._parquet.ParquetSchema object at 0x7f797ba70cc0>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
  optional group field_id=-1 list_int (List) {
    repeated group field_id=-1 list {
      optional int64 field_id=-1 element;
    }
  }
  optional group field_id=-1 list_float (List) {
    repeated group field_id=-1 list {
      optional double field_id=-1 element;
    }
  }
  optional group field_id=-1 list_str (List) {
    repeated group field_id=-1 list {
      optional binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 list_struct {
    optional binary field_id=-1 name (String);
    optional int64 field_id=-1 score;
  }
}


data:
pyarrow.Table
idx: int32
name: large_string
age: int64
sex: bool
weight: double
atime: timestamp[us]
adate: date32[day]
list_int: large_list<element: int64>
  child 0, element: int64
list_float: large_list<element: double>
  child 0, element: double
list_str: large_list<element: large_string>
  child 0, element: large_string
list_struct: struct<name: large_string, score: int64>
  child 0, name: large_string
  child 1, score: int64
----
idx: [[0,1]]
name: [["azerty","qwerty"]]
age: [[22,23]]
sex: [[true,false]]
weight: [[51.2,65.3]]
atime: [[2023-02-04 19:05:26.495201,2023-02-04 19:05:26.495201]]
adate: [[2023-02-04,2023-02-04]]
list_int: [[[10,20],[11,22]]]
list_float: [[[100.5,200.1],[115.8,225.4]]]
list_str: [[["a","b"],["x","y"]]]
...

******************** N=500000 filename=sample-big-a.pqt
----------
columns:
[Int32, Utf8, Int64, Boolean, Float64, Datetime(tu='us', tz=None), Date]
df:
shape: (1000000, 7)
┌────────┬────────┬─────┬───────┬────────┬────────────────────────────┬────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      ┆ adate      │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        ┆ ---        │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               ┆ date       │
╞════════╪════════╪═════╪═══════╪════════╪════════════════════════════╪════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ...    ┆ ...                        ┆ ...        │
│ 999996 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
│ 999997 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
│ 999998 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
│ 999999 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:05:26.498052 ┆ 2023-02-04 │
└────────┴────────┴─────┴───────┴────────┴────────────────────────────┴────────────┘
df built in 2.26 s

df saved in 0.09 s: sample-big-a.pqt

----------
schema sample-big-a.pqt:
<pyarrow._parquet.ParquetSchema object at 0x7f797ba73a80>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
}


******************** N=500000 filename=sample-big-b.pqt
----------
columns:
[Int32, Utf8, Int64, Boolean, Float64, Datetime(tu='us', tz=None), Date, List(Int64), List(Float64), List(Utf8), Struct([Field('name': Utf8), Field('score': Int64)])]
df:
shape: (1000000, 11)
┌────────┬────────┬─────┬───────┬─────┬───────────┬────────────────┬────────────┬───────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ ... ┆ list_int  ┆ list_float     ┆ list_str   ┆ list_struct   │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆     ┆ ---       ┆ ---            ┆ ---        ┆ ---           │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆     ┆ list[i64] ┆ list[f64]      ┆ list[str]  ┆ struct[2]     │
╞════════╪════════╪═════╪═══════╪═════╪═══════════╪════════════════╪════════════╪═══════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ... ┆ ...       ┆ ...            ┆ ...        ┆ ...           │
│ 999996 ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 999997 ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ 999998 ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 999999 ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
└────────┴────────┴─────┴───────┴─────┴───────────┴────────────────┴────────────┴───────────────┘
df built in 2.17 s

df saved in 0.26 s: sample-big-b.pqt

----------
schema sample-big-b.pqt:
<pyarrow._parquet.ParquetSchema object at 0x7f797ba737c0>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
  optional group field_id=-1 list_int (List) {
    repeated group field_id=-1 list {
      optional int64 field_id=-1 element;
    }
  }
  optional group field_id=-1 list_float (List) {
    repeated group field_id=-1 list {
      optional double field_id=-1 element;
    }
  }
  optional group field_id=-1 list_str (List) {
    repeated group field_id=-1 list {
      optional binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 list_struct {
    optional binary field_id=-1 name (String);
    optional int64 field_id=-1 score;
  }
}
```

## Read and write parquet from go

### fraugster/parquet-go

Output:

```sh
❯ go run ./main.go
--------------
open file sample-small-b.pqt
--------------
schema:
message schema {
  optional int32 idx;
  optional binary name (STRING);
  optional int64 age;
  optional boolean sex;
  optional double weight;
  optional int64 atime (TIMESTAMP(MICROS, false));
  optional int32 adate (DATE);
  optional group list_int (LIST) {
    repeated group list {
      optional int64 element;
    }
  }
  optional group list_float (LIST) {
    repeated group list {
      optional double element;
    }
  }
  optional group list_str (LIST) {
    repeated group list {
      optional binary element (STRING);
    }
  }
  optional group list_struct {
    optional binary name (STRING);
    optional int64 score;
  }
}

--------------
start parse rows
read time 932.886µs
nb rows 2
--------------
first rows:
{0 azerty 22 true 51.2 [10 20] [100.5 200.1] [a b] 1675537691977830 2023-02-04 20:08:11.97783 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {London 15}}
{1 qwerty 23 false 65.3 [11 22] [115.8 225.4] [x y] 1675537691977830 2023-02-04 20:08:11.97783 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {Rome 17}}
--------------
parse schema definition
--------------
start write rows
saved in 2.204721ms: sample-small-b-2.pqt
--------------
open file sample-big-b.pqt
--------------
schema:
message schema {
  optional int32 idx;
  optional binary name (STRING);
  optional int64 age;
  optional boolean sex;
  optional double weight;
  optional int64 atime (TIMESTAMP(MICROS, false));
  optional int32 adate (DATE);
  optional group list_int (LIST) {
    repeated group list {
      optional int64 element;
    }
  }
  optional group list_float (LIST) {
    repeated group list {
      optional double element;
    }
  }
  optional group list_str (LIST) {
    repeated group list {
      optional binary element (STRING);
    }
  }
  optional group list_struct {
    optional binary name (STRING);
    optional int64 score;
  }
}

--------------
start parse rows
read time 8.128397167s
nb rows 1000000
--------------
first rows:
{0 azerty 22 true 51.2 [10 20] [100.5 200.1] [a b] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {London 15}}
{1 qwerty 23 false 65.3 [11 22] [115.8 225.4] [x y] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {Rome 17}}
{2 azerty 22 true 51.2 [10 20] [100.5 200.1] [a b] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {London 15}}
{3 qwerty 23 false 65.3 [11 22] [115.8 225.4] [x y] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {Rome 17}}
{4 azerty 22 true 51.2 [10 20] [100.5 200.1] [a b] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {London 15}}
{5 qwerty 23 false 65.3 [11 22] [115.8 225.4] [x y] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {Rome 17}}
{6 azerty 22 true 51.2 [10 20] [100.5 200.1] [a b] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {London 15}}
{7 qwerty 23 false 65.3 [11 22] [115.8 225.4] [x y] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {Rome 17}}
{8 azerty 22 true 51.2 [10 20] [100.5 200.1] [a b] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {London 15}}
{9 qwerty 23 false 65.3 [11 22] [115.8 225.4] [x y] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {Rome 17}}
{10 azerty 22 true 51.2 [10 20] [100.5 200.1] [a b] 1675537694295833 2023-02-04 20:08:14.295833 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC {London 15}}
--------------
parse schema definition
--------------
start write rows
saved in 8.117551205s: sample-big-b-2.pqt
```

### xitongsys/parquet-go

Output:

```sh
❯ go run ./main.go
--------------
open file ../parquet/sample-small-a.pqt
read time 296.608µs
nb rows 2
--------------
first rows:
{0 azerty 22 true 51.2 1675537691758805 2023-02-04 20:08:11.758805 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{1 qwerty 23 false 65.3 1675537691758805 2023-02-04 20:08:11.758805 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
--------------
start write rows
saved in 1.114926ms: sample-small-a-2.pqt
--------------
open file ../parquet/sample-big-a.pqt
read time 1.296489854s
nb rows 1000000
--------------
first rows:
{0 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{1 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{2 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{3 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{4 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{5 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{6 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{7 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{8 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{9 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{10 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
--------------
start write rows
saved in 2.870319981s: sample-big-a-2.pqt
```

### segmentio/parquet-go

Output:

```sh
❯ go run ./main.go
--------------
open file ../parquet/sample-small-a.pqt
read time 433.013µs
nb rows 2
--------------
first rows:
{0 azerty 22 true 51.2 1675537691758805 19392 [0]}
{1 qwerty 23 false 65.3 1675537691758805 19392 [0]}
--------------
start write rows
saved in 890.68µs: sample-small-a-3.pqt
--------------
open file ../parquet/sample-big-a.pqt
read time 1.220483499s
nb rows 1000000
--------------
first rows:
{0 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{1 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{2 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{3 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{4 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{5 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{6 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{7 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{8 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{9 qwerty 23 false 65.3 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
{10 azerty 22 true 51.2 1675537691980925 2023-02-04 20:08:11.980925 +0100 CET 19392 2023-02-04 00:00:00 +0000 UTC}
--------------
start write rows
saved in 2.82346381s: sample-big-a-3.pqt
```

## Read parquet from polars

### From fraugster/parquet-go

Output:

```sh
❯ python read-parquet.py
read file sample-small-b.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date, 'list_int': List(Int64), 'list_float': List(Float64), 'list_str': List(Utf8), 'list_struct': Struct([Field('name': Utf8), Field('score': Int64)])}
shape: (2, 6)
┌─────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ --- ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞═════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.977830 │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.977830 │
└─────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (2, 5)
┌────────────┬───────────┬────────────────┬────────────┬───────────────┐
│ adate      ┆ list_int  ┆ list_float     ┆ list_str   ┆ list_struct   │
│ ---        ┆ ---       ┆ ---            ┆ ---        ┆ ---           │
│ date       ┆ list[i64] ┆ list[f64]      ┆ list[str]  ┆ struct[2]     │
╞════════════╪═══════════╪════════════════╪════════════╪═══════════════╡
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
└────────────┴───────────┴────────────────┴────────────┴───────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f6a46442b00>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
  optional group field_id=-1 list_int (List) {
    repeated group field_id=-1 list {
      optional int64 field_id=-1 element;
    }
  }
  optional group field_id=-1 list_float (List) {
    repeated group field_id=-1 list {
      optional double field_id=-1 element;
    }
  }
  optional group field_id=-1 list_str (List) {
    repeated group field_id=-1 list {
      optional binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 list_struct {
    optional binary field_id=-1 name (String);
    optional int64 field_id=-1 score;
  }
}

read file sample-small-b-2.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date, 'list_int': List(Int64), 'list_float': List(Float64), 'list_str': List(Utf8), 'list_struct': Struct([Field('name': Utf8), Field('score': Int64)])}
shape: (2, 6)
┌─────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ --- ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞═════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.977830 │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.977830 │
└─────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (2, 5)
┌────────────┬───────────┬────────────────┬────────────┬───────────────┐
│ adate      ┆ list_int  ┆ list_float     ┆ list_str   ┆ list_struct   │
│ ---        ┆ ---       ┆ ---            ┆ ---        ┆ ---           │
│ date       ┆ list[i64] ┆ list[f64]      ┆ list[str]  ┆ struct[2]     │
╞════════════╪═══════════╪════════════════╪════════════╪═══════════════╡
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
└────────────┴───────────┴────────────────┴────────────┴───────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f6a68113240>
required group field_id=-1 schema {
  required int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
  optional group field_id=-1 list_int (List) {
    repeated group field_id=-1 list {
      optional int64 field_id=-1 element;
    }
  }
  optional group field_id=-1 list_float (List) {
    repeated group field_id=-1 list {
      optional double field_id=-1 element;
    }
  }
  optional group field_id=-1 list_str (List) {
    repeated group field_id=-1 list {
      optional binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 list_struct {
    optional binary field_id=-1 name (String);
    optional int64 field_id=-1 score;
  }
}

read file sample-big-b.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date, 'list_int': List(Int64), 'list_float': List(Float64), 'list_str': List(Utf8), 'list_struct': Struct([Field('name': Utf8), Field('score': Int64)])}
shape: (1000000, 6)
┌────────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞════════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ...    ┆ ...                        │
│ 999996 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
│ 999997 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
│ 999998 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
│ 999999 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
└────────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (1000000, 5)
┌────────────┬───────────┬────────────────┬────────────┬───────────────┐
│ adate      ┆ list_int  ┆ list_float     ┆ list_str   ┆ list_struct   │
│ ---        ┆ ---       ┆ ---            ┆ ---        ┆ ---           │
│ date       ┆ list[i64] ┆ list[f64]      ┆ list[str]  ┆ struct[2]     │
╞════════════╪═══════════╪════════════════╪════════════╪═══════════════╡
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ ...        ┆ ...       ┆ ...            ┆ ...        ┆ ...           │
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
└────────────┴───────────┴────────────────┴────────────┴───────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f6a681131c0>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
  optional group field_id=-1 list_int (List) {
    repeated group field_id=-1 list {
      optional int64 field_id=-1 element;
    }
  }
  optional group field_id=-1 list_float (List) {
    repeated group field_id=-1 list {
      optional double field_id=-1 element;
    }
  }
  optional group field_id=-1 list_str (List) {
    repeated group field_id=-1 list {
      optional binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 list_struct {
    optional binary field_id=-1 name (String);
    optional int64 field_id=-1 score;
  }
}

read file sample-big-b-2.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date, 'list_int': List(Int64), 'list_float': List(Float64), 'list_str': List(Utf8), 'list_struct': Struct([Field('name': Utf8), Field('score': Int64)])}
shape: (500001, 6)
┌────────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞════════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ...    ┆ ...                        │
│ 499997 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
│ 499998 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
│ 499999 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:14.295833 │
│ 500000 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:14.295833 │
└────────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (500001, 5)
┌────────────┬───────────┬────────────────┬────────────┬───────────────┐
│ adate      ┆ list_int  ┆ list_float     ┆ list_str   ┆ list_struct   │
│ ---        ┆ ---       ┆ ---            ┆ ---        ┆ ---           │
│ date       ┆ list[i64] ┆ list[f64]      ┆ list[str]  ┆ struct[2]     │
╞════════════╪═══════════╪════════════════╪════════════╪═══════════════╡
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ ...        ┆ ...       ┆ ...            ┆ ...        ┆ ...           │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
│ 2023-02-04 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ ["x", "y"] ┆ {"Rome",17}   │
│ 2023-02-04 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ ["a", "b"] ┆ {"London",15} │
└────────────┴───────────┴────────────────┴────────────┴───────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f6a68111ec0>
required group field_id=-1 schema {
  required int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
  optional group field_id=-1 list_int (List) {
    repeated group field_id=-1 list {
      optional int64 field_id=-1 element;
    }
  }
  optional group field_id=-1 list_float (List) {
    repeated group field_id=-1 list {
      optional double field_id=-1 element;
    }
  }
  optional group field_id=-1 list_str (List) {
    repeated group field_id=-1 list {
      optional binary field_id=-1 element (String);
    }
  }
  optional group field_id=-1 list_struct {
    optional binary field_id=-1 name (String);
    optional int64 field_id=-1 score;
  }
}
```

### From xitongsys/parquet-go

Output:

```sh
❯ python read-parquet.py
read file sample-small-a.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date}
shape: (2, 6)
┌─────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ --- ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞═════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.758805 │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.758805 │
└─────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (2, 1)
┌────────────┐
│ adate      │
│ ---        │
│ date       │
╞════════════╡
│ 2023-02-04 │
│ 2023-02-04 │
└────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f807c4a0e40>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
}

read file sample-small-a-2.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float32, 'atime': Int64, 'adate': Date}
shape: (2, 6)
┌─────┬────────┬─────┬───────┬───────────┬────────────────────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ weight    ┆ atime                      │
│ --- ┆ ---    ┆ --- ┆ ---   ┆ ---       ┆ ---                        │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆ f32       ┆ datetime[μs]               │
╞═════╪════════╪═════╪═══════╪═══════════╪════════════════════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.758805 │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.758805 │
└─────┴────────┴─────┴───────┴───────────┴────────────────────────────┘
shape: (2, 1)
┌────────────┐
│ adate      │
│ ---        │
│ date       │
╞════════════╡
│ 2023-02-04 │
│ 2023-02-04 │
└────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f807c48fa40>
required group field_id=-1 parquet_go_root {
  required int32 field_id=0 idx;
  required binary field_id=0 name (String);
  required int64 field_id=0 age;
  required boolean field_id=0 sex;
  required float field_id=0 weight;
  required int64 field_id=0 atime;
  required int32 field_id=0 adate (Date);
}

read file sample-big-a.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date}
shape: (1000000, 6)
┌────────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞════════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ...    ┆ ...                        │
│ 999996 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 999997 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
│ 999998 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 999999 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
└────────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (1000000, 1)
┌────────────┐
│ adate      │
│ ---        │
│ date       │
╞════════════╡
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ ...        │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
└────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f807c6f5280>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
}

read file sample-big-a-2.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float32, 'atime': Int64, 'adate': Date}
shape: (1000000, 6)
┌────────┬────────┬─────┬───────┬───────────┬────────────────────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ weight    ┆ atime                      │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆ ---       ┆ ---                        │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆ f32       ┆ datetime[μs]               │
╞════════╪════════╪═════╪═══════╪═══════════╪════════════════════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ...       ┆ ...                        │
│ 999996 ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 999997 ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
│ 999998 ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 999999 ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
└────────┴────────┴─────┴───────┴───────────┴────────────────────────────┘
shape: (1000000, 1)
┌────────────┐
│ adate      │
│ ---        │
│ date       │
╞════════════╡
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ ...        │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
└────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f807c6f4e40>
required group field_id=-1 parquet_go_root {
  required int32 field_id=0 idx;
  required binary field_id=0 name (String);
  required int64 field_id=0 age;
  required boolean field_id=0 sex;
  required float field_id=0 weight;
  required int64 field_id=0 atime;
  required int32 field_id=0 adate (Date);
}
```

### From segmentio/parquet-go

Output:

```sh
❯ python read-parquet.py
read file sample-small-a.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date}
shape: (2, 6)
┌─────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ --- ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞═════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.758805 │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.758805 │
└─────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (2, 1)
┌────────────┐
│ adate      │
│ ---        │
│ date       │
╞════════════╡
│ 2023-02-04 │
│ 2023-02-04 │
└────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f04feec17c0>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
}

read file sample-small-a-3.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Int64, 'adate': Int32, 'list_int': List(Int64)}
shape: (2, 6)
┌─────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ --- ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞═════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.758805 │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.758805 │
└─────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (2, 2)
┌────────────┬───────────┐
│ adate      ┆ list_int  │
│ ---        ┆ ---       │
│ date       ┆ list[i64] │
╞════════════╪═══════════╡
│ 2023-02-04 ┆ []        │
│ 2023-02-04 ┆ []        │
└────────────┴───────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f04feeabc80>
required group field_id=-1 Row {
  required int32 field_id=-1 idx (Int(bitWidth=32, isSigned=true));
  required binary field_id=-1 name (String);
  required int64 field_id=-1 age (Int(bitWidth=64, isSigned=true));
  required boolean field_id=-1 sex;
  required double field_id=-1 weight;
  required int64 field_id=-1 atime (Int(bitWidth=64, isSigned=true));
  required int32 field_id=-1 adate (Int(bitWidth=32, isSigned=true));
  repeated int64 field_id=-1 list_int (Int(bitWidth=64, isSigned=true));
}

read file sample-big-a.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Datetime(tu='us', tz=None), 'adate': Date}
shape: (1000000, 6)
┌────────┬────────┬─────┬───────┬────────┬────────────────────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ weight ┆ atime                      │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆ ---    ┆ ---                        │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆ f64    ┆ datetime[μs]               │
╞════════╪════════╪═════╪═══════╪════════╪════════════════════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ...    ┆ ...                        │
│ 999996 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 999997 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
│ 999998 ┆ azerty ┆ 22  ┆ true  ┆ 51.2   ┆ 2023-02-04 19:08:11.980925 │
│ 999999 ┆ qwerty ┆ 23  ┆ false ┆ 65.3   ┆ 2023-02-04 19:08:11.980925 │
└────────┴────────┴─────┴───────┴────────┴────────────────────────────┘
shape: (1000000, 1)
┌────────────┐
│ adate      │
│ ---        │
│ date       │
╞════════════╡
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ ...        │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
└────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f04feec2040>
required group field_id=-1 schema {
  optional int32 field_id=-1 idx;
  optional binary field_id=-1 name (String);
  optional int64 field_id=-1 age;
  optional boolean field_id=-1 sex;
  optional double field_id=-1 weight;
  optional int64 field_id=-1 atime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int32 field_id=-1 adate (Date);
}

read file sample-big-a-3.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float32, 'atime': Int64, 'adate': Date}
shape: (1000000, 6)
┌────────┬────────┬─────┬───────┬───────────┬────────────────────────────┐
│ idx    ┆ name   ┆ age ┆ sex   ┆ weight    ┆ atime                      │
│ ---    ┆ ---    ┆ --- ┆ ---   ┆ ---       ┆ ---                        │
│ i32    ┆ str    ┆ i64 ┆ bool  ┆ f32       ┆ datetime[μs]               │
╞════════╪════════╪═════╪═══════╪═══════════╪════════════════════════════╡
│ 0      ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 1      ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
│ 2      ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 3      ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
│ ...    ┆ ...    ┆ ... ┆ ...   ┆ ...       ┆ ...                        │
│ 999996 ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 999997 ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
│ 999998 ┆ azerty ┆ 22  ┆ true  ┆ 51.200001 ┆ 2023-02-04 19:08:11.980925 │
│ 999999 ┆ qwerty ┆ 23  ┆ false ┆ 65.300003 ┆ 2023-02-04 19:08:11.980925 │
└────────┴────────┴─────┴───────┴───────────┴────────────────────────────┘
shape: (1000000, 1)
┌────────────┐
│ adate      │
│ ---        │
│ date       │
╞════════════╡
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ ...        │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
│ 2023-02-04 │
└────────────┘
<pyarrow._parquet.ParquetSchema object at 0x7f04fee5f2c0>
required group field_id=-1 parquet_go_root {
  required int32 field_id=0 idx;
  required binary field_id=0 name (String);
  required int64 field_id=0 age;
  required boolean field_id=0 sex;
  required float field_id=0 weight;
  required int64 field_id=0 atime;
  required int32 field_id=0 adate (Date);
}
```
