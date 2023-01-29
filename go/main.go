package main

import pqtest "pqtest/demo"

func main() {

	pqtest.ReadWriteAllCols(
		"sample-small.pqt",
		"sample-small-2.pqt",
	)

	pqtest.ReadWriteAllCols(
		"sample-big.pqt",
		"sample-big-2.pqt",
	)

}
