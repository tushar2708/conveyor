package squaring_numbers

import (
	"fmt"
	"github.com/tushar2708/conveyor"
)

func getInitialConveyor() (*conveyor.Conveyor, error) {
	cnv, err := conveyor.NewConveyor("square_printer", 10)
	if err != nil {
		return nil, err
	}
	return cnv, nil
}

func PrepareLoopingConveyor() (*conveyor.Conveyor, error) {

	cnv, cErr := getInitialConveyor()
	if cErr != nil {
		return nil, cErr
	}

	// Create a source executor, and add it to conveyor.
	// A pipeline may have only one source node
	gen := &NumberSource{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{
			Name: "number_generator",
		},
		CountLimit: 20,
	}
	if err := cnv.AddNodeExecutor(gen, conveyor.WorkerModeLoop, conveyor.WorkerTypeSource); err != nil {
		fmt.Printf("Failed to add NumberSource to conveyor :[+%v]", err)
	}

	// Create an operation executor, and add it to conveyor.
	// A pipeline may have many operation nodes
	sqr := &SquareOperator{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{
			Name: "number_squarer",
		},
	}
	if err := cnv.AddNodeExecutor(sqr, conveyor.WorkerModeLoop, conveyor.WorkerTypeOperation); err != nil {
		fmt.Printf("Failed to add SquareOperator to conveyor :[+%v]", err)
	}

	// Create a sink executor, and add it to conveyor.
	// A pipeline may have only multiple sink nodes, however this example shows only one.
	// Look for the 3rd example to see how you can stream same data to multiple sinks using a joint
	prnt := &PrinterSink{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{
			Name: "number_printer",
		},
	}
	if err := cnv.AddNodeExecutor(prnt, conveyor.WorkerModeLoop, conveyor.WorkerTypeSink); err != nil {
		fmt.Printf("Failed to add PrinterSink1 to conveyor :[+%v]", err)
	}

	return cnv, nil
}

func PrepareTransactionalConveyor() (*conveyor.Conveyor, error) {

	cnv, cErr := getInitialConveyor()
	if cErr != nil {
		return nil, cErr
	}

	gen := &NumberSource{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{Name: "number_generator",},
		CountLimit:           20,
	}

	if err := cnv.AddNodeExecutor(gen, conveyor.WorkerModeLoop, conveyor.WorkerTypeSource); err != nil {
		fmt.Printf("Failed to add NumberSource to conveyor :[+%v]", err)
	}

	sqr := &SquareOperator{ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{Name: "number_squarer",},}

	if err := cnv.AddNodeExecutor(sqr, conveyor.WorkerModeLoop, conveyor.WorkerTypeOperation); err != nil {
		fmt.Printf("Failed to add SquareOperator to conveyor :[+%v]", err)
	}

	prnt := &PrinterSink{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{
			Name: "number_printer",
		},
	}

	if err := cnv.AddNodeExecutor(prnt, conveyor.WorkerModeLoop, conveyor.WorkerTypeSink); err != nil {
		fmt.Printf("Failed to add PrinterSink1 to conveyor :[+%v]", err)
	}

	return cnv, nil
}

func PrepareComplexTransactionalConveyor() (*conveyor.Conveyor, error) {

	cnv, cErr := getInitialConveyor()
	if cErr != nil {
		return nil, cErr
	}

	gen := &NumberSource{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{
			Name: "number_generator",
		},
		CountLimit: 20,
	}

	if err := cnv.AddNodeExecutor(gen, conveyor.WorkerModeLoop, conveyor.WorkerTypeSource); err != nil {
		fmt.Printf("Failed to add NumberSource to conveyor :[+%v]", err)
		return nil, err
	}

	sqr := &SquareOperator{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{
			Name: "number_squarer",
		},
	}

	if err := cnv.AddNodeExecutor(sqr, conveyor.WorkerModeTransaction, conveyor.WorkerTypeOperation); err != nil {
		fmt.Printf("Failed to add SquareOperator to conveyor :[+%v]", err)
		return nil, err
	}

	addr := &AdditionOperator{
		ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{Name: "number_adder",},
		ToAdd:                5,
	}

	if err := cnv.AddNodeExecutor(addr, conveyor.WorkerModeTransaction, conveyor.WorkerTypeOperation); err != nil {
		fmt.Printf("Failed to add AdditionOperator to conveyor :[+%v]", err)
	}

	/*
	Create a replication joint, and add it to conveyor using "AddJointExecutorAfterNode" method
	  */
	joint, jntErr := conveyor.NewReplicateJoint("replicator", 3)
	if jntErr != nil {
		fmt.Printf("Failed to add SquareOperator to conveyor :[+%v]", jntErr)
		return nil, jntErr
	}

	if err := cnv.AddJointExecutorAfterNode(joint, conveyor.WorkerModeLoop, conveyor.WorkerTypeJoint); err != nil {
		return nil, err
	}

	/*
	Create 3 sink nodes, and add it to conveyor using "AddNodeExecutorAfterJoint" method.
	Make sure that you are adding this method, and not "AddNodeExecutor"
	whenever you need to connect a "Node" next to a "Joint"
	  */

	prnt1 := &PrinterSink1{
		PrinterSink: &PrinterSink{ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{Name: "number_printer"}},
	}
	if err := cnv.AddNodeExecutorAfterJoint(prnt1, conveyor.WorkerModeTransaction, conveyor.WorkerTypeSink); err != nil {
		fmt.Printf("Failed to add PrinterSink1 to conveyor :[+%v]", err)
	}

	prnt2 := &PrinterSink2{
		PrinterSink: &PrinterSink{ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{Name: "number_printer"}},
	}
	if err := cnv.AddNodeExecutorAfterJoint(prnt2, conveyor.WorkerModeTransaction, conveyor.WorkerTypeSink); err != nil {
		fmt.Printf("Failed to add PrinterSink2 to conveyor :[+%v]", err)
	}

	prnt3 := &PrinterSink3{
		PrinterSink: &PrinterSink{ConcreteNodeExecutor: &conveyor.ConcreteNodeExecutor{Name: "number_printer"}},
	}
	if err := cnv.AddNodeExecutorAfterJoint(prnt3, conveyor.WorkerModeTransaction, conveyor.WorkerTypeSink); err != nil {
		fmt.Printf("Failed to add PrinterSink3 to conveyor :[+%v]", err)
	}

	return cnv, nil
}
