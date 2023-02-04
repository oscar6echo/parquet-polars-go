package pqtest

import (
	"fmt"
	"log"
	"time"

	_ "github.com/akrennmair/parquet-go-zstd" // registers the Zstd block compressor with parquet-go
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

func Fraugster_ReadWrite(filenameIn string, filenameOut string) {
	parquetFileIn := "../parquet/" + filenameIn
	parquetFileOut := "../parquet/" + filenameOut

	t0 := time.Now()

	fmt.Println("--------------")
	fmt.Printf("open file %s\n", filenameIn)

	fr, err := floor.NewFileReader(parquetFileIn)
	if err != nil {
		log.Fatalf("Opening parquet file for reading failed: %v", err)
	}

	fmt.Println("--------------")
	fmt.Println("schema:")
	fmt.Println(fr.GetSchemaDefinition())

	fmt.Println("--------------")
	fmt.Println("start parse rows")

	var rows []record

	for fr.Next() {
		var rec record
		if err := fr.Scan(&rec); err != nil {
			log.Fatalf("Scanning record failed: %v", err)
		}
		rows = append(rows, rec)
	}

	readtime := time.Since(t0)
	fmt.Printf("read time %s\n", readtime)
	fmt.Printf("nb rows %d\n", len(rows))
	fmt.Println("--------------")
	fmt.Println("first rows:")
	for c, row := range rows {
		if c > 10 {
			break
		}
		fmt.Println(row)
	}

	fmt.Println("--------------")
	fmt.Println("parse schema definition")

	schemaDef, err := parquetschema.ParseSchemaDefinition(
		`message schema {
			required int32 idx;
			optional binary name (String);
			optional int64 age;
			optional boolean sex;
			optional double weight;
			optional int64 atime (TIMESTAMP(MICROS,false));
			optional int32 adate (Date);
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
				optional binary element (String);
				}
			}
			optional group list_struct {
				optional binary name (String);
				optional int64 score;
			}
		}`)
	if err != nil {
		log.Fatalf("Parsing schema definition failed: %v", err)
	}
	// fmt.Println("schema:")
	// fmt.Println(schemaDef)

	fw, err := floor.NewFileWriter(parquetFileOut,
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_ZSTD),
	)
	if err != nil {
		log.Fatalf("Opening parquet file for writing failed: %v", err)
	}

	fmt.Println("--------------")
	fmt.Println("start write rows")
	t0 = time.Now()

	for i, rec := range rows {

		if i > len(rows)/2 {
			break
		}
		if err := fw.Write(rec); err != nil {
			log.Fatalf("Writing record failed: %v", err)
		}
	}

	if err := fw.Close(); err != nil {
		log.Fatalf("Closing parquet writer failed: %v", err)
	}

	readtime = time.Since(t0)
	fmt.Printf("saved in %s: %s\n", readtime, filenameOut)

}

type city struct {
	name  string
	score int64
}

type record struct {
	Idx       int32     `parquet:"idx"`
	Name      string    `parquet:"name"`
	Age       int64     `parquet:"age"`
	Sex       bool      `parquet:"sex"`
	Weight    float64   `parquet:"weight"`
	Listint   []int64   `parquet:"list_int"`
	Listfloat []float64 `parquet:"list_float"`
	Liststr   []string  `parquet:"list_str"`

	Atime  int64 `parquet:"atime"`
	Atime2 time.Time
	Adate  int32 `parquet:"adate"`
	Adate2 time.Time

	Liststruct city `parquet:"list_struct"`
}

// func (r *record) MarshalParquet(obj interfaces.MarshalObject) error {
// UNECCESSARY - surprisingly...
// 	return nil
// }

func (r *record) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	idx, err := obj.GetField("idx").Int32()
	if err != nil {
		return err
	}
	r.Idx = idx

	name, err := obj.GetField("name").ByteArray()
	if err != nil {
		return err
	}
	r.Name = string(name)

	age, err := obj.GetField("age").Int64()
	if err != nil {
		return err
	}
	r.Age = age

	sex, err := obj.GetField("sex").Bool()
	if err != nil {
		return err
	}
	r.Sex = sex

	weight, err := obj.GetField("weight").Float64()
	if err != nil {
		return err
	}
	r.Weight = weight

	atime, err := obj.GetField("atime").Int64()
	if err != nil {
		return err
	}
	r.Atime = atime
	// >>> convert to time[micro sec] to time
	r.Atime2 = time.Unix(0, atime*1000)

	adate, err := obj.GetField("adate").Int32()
	if err != nil {
		return err
	}
	r.Adate = adate
	// >>> convert date in days since unix epoch to time
	origin := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	r.Adate2 = origin.AddDate(0, 0, int(adate))

	listint, err := extractListInt(obj, "list_int")
	if err != nil {
		return err
	}
	r.Listint = listint

	listfloat, err := extractListFloat(obj, "list_float")
	if err != nil {
		return err
	}
	r.Listfloat = listfloat

	liststr, err := extractListString(obj, "list_str")
	if err != nil {
		return err
	}
	r.Liststr = liststr

	list_struct, err := obj.GetField("list_struct").Group()
	if err != nil {
		return err
	}

	_name := list_struct.GetField("name")
	__name, e1 := _name.ByteArray()
	if e1 != nil {
		return e1
	}

	_score := list_struct.GetField("score")
	__score, e1 := _score.Int64()
	if e1 != nil {
		return e1
	}

	nc := city{
		name:  string(__name),
		score: __score,
	}

	r.Liststruct = nc

	return nil
}
