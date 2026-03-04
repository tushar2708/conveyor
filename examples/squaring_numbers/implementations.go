package squaringNumbers

import (
	"fmt"

	"github.com/tushar2708/conveyor"
)

/*
Count() decides how many goroutines will run for the node.
If Count() returns a number greater than 1, Execute and ExecuteLoop must be
race-free, either by using locks or by isolating state per goroutine.
*/

// Count of go-routines for source node
// (kept as one because the CountLimit counter is not race-safe)
func (src *NumberSource) Count() int {
	return 1
}

// Count of go-routines for operation node
// (3 goroutines allowed; Execute does not touch shared state)
func (oper *SquareOperator) Count() int {
	return 3
}

// Count of go-routines for sink node
// (5 goroutines allowed; Execute does not touch shared state)
func (snk *PrinterSink) Count() int {
	return 5
}

// ExecuteLoop (source) streams numbers 0..CountLimit to outChan.
// Used when the executor is added with WorkerModeLoop.
func (src *NumberSource) ExecuteLoop(ctx conveyor.CnvContext, outChan chan<- int) error {
	for i := 0; i <= src.CountLimit; i++ {
		outChan <- i
	}
	return nil
}

// ExecuteLoop (operator) squares each number read from inChan and sends it to outChan.
// Used when the executor is added with WorkerModeLoop.
func (oper *SquareOperator) ExecuteLoop(ctx conveyor.CnvContext, inChan <-chan int, outChan chan<- int) error {
	for num := range inChan {
		outChan <- num * num
	}
	return nil
}

// ExecuteLoop (sink) prints each number read from inChan.
// Used when the executor is added with WorkerModeLoop.
func (snk *PrinterSink) ExecuteLoop(ctx conveyor.CnvContext, inChan <-chan int) error {
	for num := range inChan {
		fmt.Printf("Output: %d\n", num)
	}
	return nil
}

// Execute (source) returns the next number in the countdown sequence.
// Returns ErrSourceExhausted when CountLimit drops below zero.
// Used when the executor is added with WorkerModeTransaction.
func (src *NumberSource) Execute(ctx conveyor.CnvContext) (int, error) {
	if src.CountLimit < 0 {
		return 0, conveyor.ErrSourceExhausted
	}
	num := src.CountLimit
	src.CountLimit--
	return num, nil
}

// Execute (square operator) returns the square of num.
// Used when the executor is added with WorkerModeTransaction.
func (oper *SquareOperator) Execute(ctx conveyor.CnvContext, num int) (int, error) {
	return num * num, nil
}

// Execute (addition operator) returns num plus the configured ToAdd value.
// Used when the executor is added with WorkerModeTransaction.
func (addr *AdditionOperator) Execute(ctx conveyor.CnvContext, num int) (int, error) {
	return num + addr.ToAdd, nil
}

// Execute (sink) prints the number.
// Used when the executor is added with WorkerModeTransaction.
func (snk *PrinterSink) Execute(ctx conveyor.CnvContext, num int) error {
	fmt.Printf("Output: %d\n", num)
	return nil
}

// Execute for PrinterSink1
func (snk *PrinterSink1) Execute(ctx conveyor.CnvContext, num int) error {
	fmt.Printf("PrinterSink1 ---> %d\n", num)
	return nil
}

// Count for PrinterSink1
func (snk *PrinterSink1) Count() int {
	return 5
}

// Execute for PrinterSink2
func (snk *PrinterSink2) Execute(ctx conveyor.CnvContext, num int) error {
	fmt.Printf("PrinterSink2 ------> %d\n", num)
	return nil
}

// Count for PrinterSink2
func (snk *PrinterSink2) Count() int {
	return 5
}

// Execute for PrinterSink3
func (snk *PrinterSink3) Execute(ctx conveyor.CnvContext, num int) error {
	fmt.Printf("PrinterSink3 ---------> %d\n", num)
	return nil
}

// Count for PrinterSink3
func (snk *PrinterSink3) Count() int {
	return 5
}
