package pqtest

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	parquet "github.com/segmentio/parquet-go"
)

type Row struct {
	Idx    int32   `parquet:"idx"`
	Name   string  `parquet:"name"`
	Age    int64   `parquet:"age"`
	Sex    bool    `parquet:"sex"`
	Weight float64 `parquet:"weight"`
	Atime  int64   `parquet:"atime"`
	Adate  int32   `parquet:"adate"`

	Arr []int64 `parquet:"list_int"`
}

func Segmentio_ReadWrite(filenameIn string, filenameOut string) {
	parquetFileIn := "../parquet/" + filenameIn
	parquetFileOut := "../parquet/" + filenameOut

	t0 := time.Now()
	fmt.Println("--------------")
	fmt.Printf("open file %s\n", parquetFileIn)

	rf, _ := os.Open(parquetFileIn)
	pf := parquet.NewReader(rf)

	rows := make([]Row, 0)
	for {
		var row Row
		err := pf.Read(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// >>> convert to time[micro sec] to time
		_ = time.Unix(0, row.Atime*1000)

		// >>> convert date in days since unix epoch to time
		origin := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		_ = origin.AddDate(0, 0, int(row.Adate))

		rows = append(rows, row)
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
	fmt.Println("start write rows")
	t0 = time.Now()

	if err := parquet.WriteFile(parquetFileOut, rows); err != nil {
		log.Fatal(err)
	}

	writetime := time.Since(t0)
	fmt.Printf("saved in %s: %s\n", writetime, filenameOut)

}
