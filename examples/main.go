package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/tushar2708/conveyor"
	"github.com/tushar2708/conveyor/examples/squaring_numbers"
)

func main() {

	args := len(os.Args)
	if args <= 1 {
		fmt.Println(`Missing Example Number.
		Usage: go run main.go <example number>`)
		os.Exit(1)
	}

	exampleToRun, cmdErr := strconv.Atoi(os.Args[1])
	if cmdErr != nil {
		fmt.Println(`Invalid Example Number.
		Usage: go run main.go <example (1, 2, or 3)>`)
		os.Exit(1)
	}

	var testConveyor *conveyor.Conveyor
	var cnvErr error

	basicConveyor, bErr := squaringNumbers.GetBasicConveyor("square_printer")
	if bErr != nil {
		fmt.Printf("basic conveyor creation failed for example: %d. cnvErr:[%v]\n", exampleToRun, cnvErr)
		os.Exit(1)

	}

	if exampleToRun == 1 {
		testConveyor, cnvErr = squaringNumbers.PrepareLoopingConveyor(basicConveyor)
	} else if exampleToRun == 2 {
		testConveyor, cnvErr = squaringNumbers.PrepareTransactionalConveyor(basicConveyor)
	} else if exampleToRun == 3 {
		testConveyor, cnvErr = squaringNumbers.PrepareComplexTransactionalConveyor(basicConveyor)
	} else {
		fmt.Println(`Example number is invalid.
		Usage: go run main.go <example number (1, 2, or 3)>
		1: Simple conveyor with loop mode
		2: Simple conveyor with transaction mode
		3: A conveyor with multiple operation and sink nodes`)
		os.Exit(1)
	}

	if cnvErr != nil {
		fmt.Printf("conveyor preparation failed for example: %d. cnvErr:[%v]\n", exampleToRun, cnvErr)
		os.Exit(1)
	}

	if err := testConveyor.Start(); err != nil {
		fmt.Printf("conveyor start failed for example: %d. cnvErr:[%v]\n", exampleToRun, cnvErr)
	}

	fmt.Println("All Done")

}
