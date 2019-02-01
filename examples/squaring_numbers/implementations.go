package squaring_numbers

import (
	"fmt"
	"github.com/tushar2708/conveyor"
)

/*
function Count() decides how many goroutines will be running for the source node.
You must ensure that if it returns a number greater than 1,
then "Execute" & "ExecuteLoop" methods must be race-free,
either by using locks or by isolating resources to be run independently
*/

// Count of go-routines for source node
// (kept as one, as "addition" done in Execute* methods isn't race-free)
func (src *NumberSource) Count() int {
	return 1
}

// Count of go-routines for operation node
// (3 go-routines allowed, as Execute* methods don't depend on any common resource)
func (oper *SquareOperator) Count() int {
	return 3
}

// Count of go-routines for sink node
// (5 go-routines allowed, as Execute* methods don't depend on any common resource)
func (snk *PrinterSink) Count() int {
	return 5
}

// ExecuteLoop (source) gets called by the conveyor, if the Executor is added with "WorkerModeLoop".
// In this method, you ignore "inChan" because source is supposed to generate new data
// and you should write your newly generated data to "outChan"
func (src *NumberSource) ExecuteLoop(ctx conveyor.CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error {

	// Below is just a sample code, as an actual use case you can do anything,
	// that starts streaming data to your pipeline
	// It could either be reading from a database, a file, a kafka stream, etc
	for i := 0; i <= src.CountLimit; i++ {
		outChan <- map[string]interface{}{"num": i}
	}
	return nil
}

// ExecuteLoop (operator) gets called by the conveyor, if the Executor is added with "WorkerModeLoop".
// In this method, you should read from "inChan" and write to "outChan"
func (oper *SquareOperator) ExecuteLoop(ctx conveyor.CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error {

	// Below is just a sample code, as an actual use case you can make an API call here,
	// based on data received from source node, and pass on the enriched data to the next node in conveyor
	for m := range inChan {
		num := m["num"].(int)
		sqr := num * num
		m["sqr"] = sqr
		outChan <- m
	}
	return nil
}

// ExecuteLoop (sink) gets called by the conveyor, if the Executor is added with "WorkerModeLoop".
// In this method, you should read from "inChan" and write to "outChan"
func (snk *PrinterSink) ExecuteLoop(ctx conveyor.CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error {

	// Below is just a sample code, as an actual use case you can do anything that terminates your pipeline,
	// It could either be sending data to the last API service, storing data to a database, or publishing it to kafka
	for m := range inChan {
		fmt.Printf("Output data:[+%v]\n", m)
	}
	return nil
}

// Execute (source) gets called by the conveyor, if the Executor is added with "WorkerModeTransaction".
// In this method, you ignore "inData" because source is supposed to generate new data
// and you should return your generated data as "outData"
func (src *NumberSource) Execute(ctx conveyor.CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {

	if src.CountLimit < 0 {
		return nil, conveyor.ErrSourceExhausted
	}
	outData := map[string]interface{}{"num": src.CountLimit}
	src.CountLimit -= 1

	return outData, nil
}

// Execute (square operator) gets called by the conveyor, if the Executor is added with "WorkerModeTransaction".
// In this method, you get "inData" as input, and you should return it's modified form as output
func (oper *SquareOperator) Execute(ctx conveyor.CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {

	num := inData["num"].(int)
	sqr := num * num
	inData["sqr"] = sqr

	return inData, nil
}

// Execute (addition operator) gets called by the conveyor, if the Executor is added with "WorkerModeTransaction".
// In this method, you get "inData" as input, and you should return it's modified form as output
func (addr *AdditionOperator) Execute(ctx conveyor.CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {

	sqr := inData["sqr"].(int)
	added := sqr + addr.ToAdd
	inData["added"] = added

	return inData, nil
}

// Execute (sink) gets called by the conveyor, if the Executor is added with "WorkerModeTransaction".
// In this method, you get "inData" as input, which you should take some action on (eg. printing in this case)
// Anything you return as "outData" will be ignored, because this is supposed to be the terminating node of the conveyor
func (snk *PrinterSink) Execute(ctx conveyor.CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {

	fmt.Printf("Output data:[+%v]\n", inData)
	return inData, nil
}

func (snk *PrinterSink1) Execute(ctx conveyor.CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {

	fmt.Printf("PrinterSink1 says ---> Output data:[+%v]\n", inData)
	return inData, nil
}
func (snk *PrinterSink2) Execute(ctx conveyor.CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {

	fmt.Printf("PrinterSink2 says ------> Output data:[+%v]\n", inData)
	return inData, nil
}
func (snk *PrinterSink3) Execute(ctx conveyor.CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {

	fmt.Printf("PrinterSink3 says ---------> Output data:[+%v]\n", inData)

	return inData, nil
}
