package main

import pqtest "pqtest/demo"

func main() {

	pqtest.ReadWriteAllCols(
		"sample-small-all-cols.pqt",
		"sample-small-2.pqt",
	)

	pqtest.ReadWriteAllCols(
		"sample-big-all-cols.pqt",
		"sample-big-2.pqt",
	)

}
