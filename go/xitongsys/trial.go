package demo

import (
	"fmt"
	"log"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type recordPqt struct {
	Idx    int32   `parquet:"name=idx, type=INT32"`
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age    int64   `parquet:"name=age, type=INT64"`
	Sex    bool    `parquet:"name=sex, type=BOOLEAN"`
	Weight float64 `parquet:"name=weight, type=FLOAT"`
	Atime  int64   `parquet:"name=atime, type=INT64"`
	Atime2 time.Time
	Adate  int32 `parquet:"name=adate, type=INT32, convertedtype=DATE"`
	Adate2 time.Time
}

func Xitongsys_ReadWrite(filenameIn string, filenameOut string) {
	parquetFileIn := "../parquet/" + filenameIn
	parquetFileOut := "../parquet/" + filenameOut

	t0 := time.Now()

	fmt.Println("--------------")
	fmt.Printf("open file %s\n", parquetFileIn)

	fr, err := local.NewLocalFileReader(parquetFileIn)
	if err != nil {
		log.Fatalf("Can't open file: %v", err)
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatalf("Can't create parquet reader: %v", err)
		return
	}

	num := int(pr.GetNumRows())
	rows := make([]recordPqt, num)

	if err = pr.Read(&rows); err != nil {
		log.Fatalf("Read error: %v", err)
	}

	for i := 0; i < num; i++ {

		// >>> convert to time[micro sec] to time
		rows[i].Atime2 = time.Unix(0, rows[i].Atime*1000)

		// >>> convert date in days since unix epoch to time
		origin := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		rows[i].Adate2 = origin.AddDate(0, 0, int(rows[i].Adate))
	}

	pr.ReadStop()
	fr.Close()

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

	fw, err := local.NewLocalFileWriter(parquetFileOut)
	if err != nil {
		log.Fatalf("Can't create file: %v", err)
		return
	}

	pw, err := writer.NewParquetWriter(fw, new(recordPqt), 4)
	if err != nil {
		log.Fatalf("Can't create parquet writer: %v", err)
		return
	}

	for _, row := range rows {
		if err = pw.Write(row); err != nil {
			log.Fatalf("Write error: %v", err)
			return
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Fatalf("WriteStop error: %v", err)
	}

	fw.Close()

	readtime = time.Since(t0)
	fmt.Printf("saved in %s: %s\n", readtime, filenameOut)

}
