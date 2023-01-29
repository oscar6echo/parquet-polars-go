# Python Polars <-> Parquet <-> Go

## Short

```sh
# from /py
# create and save sample polars dataframe
# with all sorts of column types incl. list and struct
# save as parquet file
python ./write_parquet.py
# output: /parquet/sample-(small|big).pqt

# from /go
# reaad parquet file and unmarshal to go struct
# save part of the data as parquet file again
go run ./main.go
# output: /parquet/sample-(small|big)-2.pqt

# from /py
# read parquet file produced by go as polars dataframe
python ./read_parquet.py
# ok :same as /parquet/sample-(small|big).pqt
```

NOTE: The parsing and writing in go is a bit slow.

## Long

- Write parquet

```sh
❯ python write-parquet.py
******************** N=1 filename=sample-small-all-cols.pqt
----------
columns:
[Int32, Utf8, Int64, Boolean, Float64, Datetime(tu='us', tz=None), Date, List(Int64), List(Float64), Struct([Field('name': Utf8), Field('score': Int64)])]
df:
shape: (2, 10)
┌─────┬────────┬─────┬───────┬─────┬────────────┬───────────┬────────────────┬───────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ ... ┆ adate      ┆ list_int  ┆ list_float     ┆ list_struct   │
│ --- ┆ ---    ┆ --- ┆ ---   ┆     ┆ ---        ┆ ---       ┆ ---            ┆ ---           │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆     ┆ date       ┆ list[i64] ┆ list[f64]      ┆ struct[2]     │
╞═════╪════════╪═════╪═══════╪═════╪════════════╪═══════════╪════════════════╪═══════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
└─────┴────────┴─────┴───────┴─────┴────────────┴───────────┴────────────────┴───────────────┘
df built in 0.21 s

df saved in 0.00 s: sample-small-all-cols.pqt

----------
schema sample-small-all-cols.pqt:
<pyarrow._parquet.ParquetSchema object at 0x7f00c933bc80>
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
list_struct: struct<name: large_string, score: int64>
  child 0, name: large_string
  child 1, score: int64
----
idx: [[0,1]]
name: [["azerty","qwerty"]]
age: [[22,23]]
sex: [[true,false]]
weight: [[51.2,65.3]]
atime: [[2023-01-29 13:08:16.576188,2023-01-29 13:08:16.576188]]
adate: [[2023-01-29,2023-01-29]]
list_int: [[[10,20],[11,22]]]
list_float: [[[100.5,200.1],[115.8,225.4]]]
list_struct: [
  -- is_valid: all not null
  -- child 0 type: large_string
["London","Rome"]
  -- child 1 type: int64
[15,17]]

******************** N=1000000 filename=sample-big-all-cols.pqt
----------
columns:
[Int32, Utf8, Int64, Boolean, Float64, Datetime(tu='us', tz=None), Date, List(Int64), List(Float64), Struct([Field('name': Utf8), Field('score': Int64)])]
df:
shape: (2000000, 10)
┌─────────┬────────┬─────┬───────┬─────┬────────────┬───────────┬────────────────┬───────────────┐
│ idx     ┆ name   ┆ age ┆ sex   ┆ ... ┆ adate      ┆ list_int  ┆ list_float     ┆ list_struct   │
│ ---     ┆ ---    ┆ --- ┆ ---   ┆     ┆ ---        ┆ ---       ┆ ---            ┆ ---           │
│ i32     ┆ str    ┆ i64 ┆ bool  ┆     ┆ date       ┆ list[i64] ┆ list[f64]      ┆ struct[2]     │
╞═════════╪════════╪═════╪═══════╪═════╪════════════╪═══════════╪════════════════╪═══════════════╡
│ 0       ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 1       ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
│ 2       ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 3       ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
│ ...     ┆ ...    ┆ ... ┆ ...   ┆ ... ┆ ...        ┆ ...       ┆ ...            ┆ ...           │
│ 1999996 ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 1999997 ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
│ 1999998 ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 1999999 ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
└─────────┴────────┴─────┴───────┴─────┴────────────┴───────────┴────────────────┴───────────────┘
df built in 4.28 s

df saved in 0.42 s: sample-big-all-cols.pqt

----------
schema sample-big-all-cols.pqt:
<pyarrow._parquet.ParquetSchema object at 0x7f00c9358980>
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
  optional group field_id=-1 list_struct {
    optional binary field_id=-1 name (String);
    optional int64 field_id=-1 score;
  }
}
```

- Read parquet in go and write other parquet file

```sh
❯ go run ./main.go
--------------
open file sample-small-all-cols.pqt
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
  optional group list_struct {
    optional binary name (STRING);
    optional int64 score;
  }
}

--------------
start parse rows
read time 1.035859ms
nb rows 2
--------------
first rows:
{0 azerty 22 true 51.2 [10 20] [100.5 200.1] 1674997696576188 2023-01-29 14:08:16.576188 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {London 15}}
{1 qwerty 23 false 65.3 [11 22] [115.8 225.4] 1674997696576188 2023-01-29 14:08:16.576188 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {Rome 17}}
--------------
parse schema definition
--------------
start write rows
saved in 2.472728ms: sample-small-2.pqt
--------------
open file sample-big-all-cols.pqt
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
  optional group list_struct {
    optional binary name (STRING);
    optional int64 score;
  }
}

--------------
start parse rows
read time 12.867989176s
nb rows 2000000
--------------
first rows:
{0 azerty 22 true 51.2 [10 20] [100.5 200.1] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {London 15}}
{1 qwerty 23 false 65.3 [11 22] [115.8 225.4] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {Rome 17}}
{2 azerty 22 true 51.2 [10 20] [100.5 200.1] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {London 15}}
{3 qwerty 23 false 65.3 [11 22] [115.8 225.4] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {Rome 17}}
{4 azerty 22 true 51.2 [10 20] [100.5 200.1] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {London 15}}
{5 qwerty 23 false 65.3 [11 22] [115.8 225.4] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {Rome 17}}
{6 azerty 22 true 51.2 [10 20] [100.5 200.1] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {London 15}}
{7 qwerty 23 false 65.3 [11 22] [115.8 225.4] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {Rome 17}}
{8 azerty 22 true 51.2 [10 20] [100.5 200.1] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {London 15}}
{9 qwerty 23 false 65.3 [11 22] [115.8 225.4] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {Rome 17}}
{10 azerty 22 true 51.2 [10 20] [100.5 200.1] 1674997696792812 2023-01-29 14:08:16.792812 +0100 CET 19386 2023-01-29 00:00:00 +0000 UTC {London 15}}
--------------
parse schema definition
--------------
start write rows
saved in 13.07200645s: sample-big-2.pqt

```

- Check go produced parquet file is readable by python polars

```sh
❯ python read-parquet.py
read file sample-small-2.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Int64, 'adate': Date, 'list_int': List(Int64), 'list_float': List(Float64), 'list_struct': Struct([Field('name': Utf8), Field('score': Int64)])}
shape: (2, 10)
┌─────┬────────┬─────┬───────┬─────┬────────────┬───────────┬────────────────┬───────────────┐
│ idx ┆ name   ┆ age ┆ sex   ┆ ... ┆ adate      ┆ list_int  ┆ list_float     ┆ list_struct   │
│ --- ┆ ---    ┆ --- ┆ ---   ┆     ┆ ---        ┆ ---       ┆ ---            ┆ ---           │
│ i32 ┆ str    ┆ i64 ┆ bool  ┆     ┆ date       ┆ list[i64] ┆ list[f64]      ┆ struct[2]     │
╞═════╪════════╪═════╪═══════╪═════╪════════════╪═══════════╪════════════════╪═══════════════╡
│ 0   ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 1   ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
└─────┴────────┴─────┴───────┴─────┴────────────┴───────────┴────────────────┴───────────────┘
read file sample-big-2.pqt
{'idx': Int32, 'name': Utf8, 'age': Int64, 'sex': Boolean, 'weight': Float64, 'atime': Int64, 'adate': Date, 'list_int': List(Int64), 'list_float': List(Float64), 'list_struct': Struct([Field('name': Utf8), Field('score': Int64)])}
shape: (1000001, 10)
┌─────────┬────────┬─────┬───────┬─────┬────────────┬───────────┬────────────────┬───────────────┐
│ idx     ┆ name   ┆ age ┆ sex   ┆ ... ┆ adate      ┆ list_int  ┆ list_float     ┆ list_struct   │
│ ---     ┆ ---    ┆ --- ┆ ---   ┆     ┆ ---        ┆ ---       ┆ ---            ┆ ---           │
│ i32     ┆ str    ┆ i64 ┆ bool  ┆     ┆ date       ┆ list[i64] ┆ list[f64]      ┆ struct[2]     │
╞═════════╪════════╪═════╪═══════╪═════╪════════════╪═══════════╪════════════════╪═══════════════╡
│ 0       ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 1       ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
│ 2       ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 3       ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
│ ...     ┆ ...    ┆ ... ┆ ...   ┆ ... ┆ ...        ┆ ...       ┆ ...            ┆ ...           │
│ 999997  ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
│ 999998  ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
│ 999999  ┆ qwerty ┆ 23  ┆ false ┆ ... ┆ 2023-01-29 ┆ [11, 22]  ┆ [115.8, 225.4] ┆ {"Rome",17}   │
│ 1000000 ┆ azerty ┆ 22  ┆ true  ┆ ... ┆ 2023-01-29 ┆ [10, 20]  ┆ [100.5, 200.1] ┆ {"London",15} │
└─────────┴────────┴─────┴───────┴─────┴────────────┴───────────┴────────────────┴───────────────┘

```
