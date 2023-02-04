package main

import (
	pqtestF "pqtest/fraugster"
	pqtestS "pqtest/segmentio"
	pqtestX "pqtest/xitongsys"
)

func main() {

	runSmallF := false
	runBigF := false

	runSmallX := false
	runBigX := false

	runSmallS := true
	runBigS := true

	if runSmallF {
		pqtestF.Fraugster_ReadWrite(
			"sample-small-b.pqt",
			"sample-small-b-2.pqt",
		)
	}
	if runBigF {
		pqtestF.Fraugster_ReadWrite(
			"sample-big-b.pqt",
			"sample-big-b-2.pqt",
		)
	}

	if runSmallX {
		pqtestX.Xitongsys_ReadWrite(
			"sample-small-a.pqt",
			"sample-small-a-2.pqt",
		)
	}
	if runBigX {
		pqtestX.Xitongsys_ReadWrite(
			"sample-big-a.pqt",
			"sample-big-a-2.pqt",
		)
	}

	if runSmallS {
		pqtestS.Segmentio_ReadWrite(
			"sample-small-a.pqt",
			"sample-small-a-3.pqt",
		)
	}
	if runBigS {
		pqtestX.Xitongsys_ReadWrite(
			"sample-big-a.pqt",
			"sample-big-a-3.pqt",
		)
	}

}
