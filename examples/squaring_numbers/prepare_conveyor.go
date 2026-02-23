package squaringNumbers

import (
	"fmt"

	"github.com/tushar2708/conveyor"
)

// GetBasicConveyor creates a basic empty conveyor
func GetBasicConveyor(name string) (*conveyor.Conveyor, error) {
	cnv, err := conveyor.NewConveyor(name, 10)
	if err != nil {
		return nil, err
	}
	return cnv, nil
}

// PrepareLoopingConveyor prepares a simple conveyor with nodes running in Loop Mode
func PrepareLoopingConveyor(cnv *conveyor.Conveyor) (*conveyor.Conveyor, error) {
	gen := &NumberSource{
		ConcreteSourceExecutor: &conveyor.ConcreteSourceExecutor[int]{
			Name: "number_generator",
		},
		CountLimit: 20,
	}
	if err := conveyor.AddSource[int](cnv, gen, conveyor.WorkerModeLoop); err != nil {
		fmt.Printf("Failed to add NumberSource: [+%v]", err)
		return nil, err
	}

	sqr := &SquareOperator{
		ConcreteOperationExecutor: &conveyor.ConcreteOperationExecutor[int, int]{
			Name: "number_squarer",
		},
	}
	if err := conveyor.AddOperation[int, int](cnv, sqr, conveyor.WorkerModeLoop); err != nil {
		fmt.Printf("Failed to add SquareOperator: [+%v]", err)
		return nil, err
	}

	prnt := &PrinterSink{
		ConcreteSinkExecutor: &conveyor.ConcreteSinkExecutor[int]{
			Name: "number_printer",
		},
	}
	if err := conveyor.AddSink[int](cnv, prnt, conveyor.WorkerModeLoop); err != nil {
		fmt.Printf("Failed to add PrinterSink: [+%v]", err)
		return nil, err
	}

	return cnv, nil
}

// PrepareTransactionalConveyor prepares a simple conveyor with transaction mode
func PrepareTransactionalConveyor(cnv *conveyor.Conveyor) (*conveyor.Conveyor, error) {
	gen := &NumberSource{
		ConcreteSourceExecutor: &conveyor.ConcreteSourceExecutor[int]{Name: "number_generator"},
		CountLimit:             20,
	}
	if err := conveyor.AddSource[int](cnv, gen, conveyor.WorkerModeLoop); err != nil {
		fmt.Printf("Failed to add NumberSource: [+%v]", err)
		return nil, err
	}

	sqr := &SquareOperator{
		ConcreteOperationExecutor: &conveyor.ConcreteOperationExecutor[int, int]{Name: "number_squarer"},
	}
	if err := conveyor.AddOperation[int, int](cnv, sqr, conveyor.WorkerModeLoop); err != nil {
		fmt.Printf("Failed to add SquareOperator: [+%v]", err)
		return nil, err
	}

	prnt := &PrinterSink{
		ConcreteSinkExecutor: &conveyor.ConcreteSinkExecutor[int]{Name: "number_printer"},
	}
	if err := conveyor.AddSink[int](cnv, prnt, conveyor.WorkerModeLoop); err != nil {
		fmt.Printf("Failed to add PrinterSink: [+%v]", err)
		return nil, err
	}

	return cnv, nil
}

// PrepareComplexTransactionalConveyor prepares a conveyor with joints and multiple sinks
func PrepareComplexTransactionalConveyor(cnv *conveyor.Conveyor) (*conveyor.Conveyor, error) {
	gen := &NumberSource{
		ConcreteSourceExecutor: &conveyor.ConcreteSourceExecutor[int]{Name: "number_generator"},
		CountLimit:             20,
	}
	if err := conveyor.AddSource[int](cnv, gen, conveyor.WorkerModeLoop); err != nil {
		fmt.Printf("Failed to add NumberSource: [+%v]", err)
		return nil, err
	}

	sqr := &SquareOperator{
		ConcreteOperationExecutor: &conveyor.ConcreteOperationExecutor[int, int]{Name: "number_squarer"},
	}
	if err := conveyor.AddOperation[int, int](cnv, sqr, conveyor.WorkerModeTransaction); err != nil {
		fmt.Printf("Failed to add SquareOperator: [+%v]", err)
		return nil, err
	}

	addr := &AdditionOperator{
		ConcreteOperationExecutor: &conveyor.ConcreteOperationExecutor[int, int]{Name: "number_adder"},
		ToAdd:                     5,
	}
	if err := conveyor.AddOperation[int, int](cnv, addr, conveyor.WorkerModeTransaction); err != nil {
		fmt.Printf("Failed to add AdditionOperator: [+%v]", err)
		return nil, err
	}

	// Create generic replicate joint that fans out to 3 output channels
	joint, jntErr := conveyor.NewReplicateJoint[int]("replicator", 3)
	if jntErr != nil {
		fmt.Printf("Failed to create ReplicateJoint: [+%v]", jntErr)
		return nil, jntErr
	}
	if err := conveyor.AddJointAfterNode[int, int](cnv, joint); err != nil {
		return nil, err
	}

	// 3 sinks wired to the joint's three output channels
	prnt1 := &PrinterSink1{
		ConcreteSinkExecutor: &conveyor.ConcreteSinkExecutor[int]{Name: "number_printer_1"},
	}
	if err := conveyor.AddSinkAfterJoint[int](cnv, prnt1, conveyor.WorkerModeTransaction); err != nil {
		fmt.Printf("Failed to add PrinterSink1: [+%v]", err)
	}

	prnt2 := &PrinterSink2{
		ConcreteSinkExecutor: &conveyor.ConcreteSinkExecutor[int]{Name: "number_printer_2"},
	}
	if err := conveyor.AddSinkAfterJoint[int](cnv, prnt2, conveyor.WorkerModeTransaction); err != nil {
		fmt.Printf("Failed to add PrinterSink2: [+%v]", err)
	}

	prnt3 := &PrinterSink3{
		ConcreteSinkExecutor: &conveyor.ConcreteSinkExecutor[int]{Name: "number_printer_3"},
	}
	if err := conveyor.AddSinkAfterJoint[int](cnv, prnt3, conveyor.WorkerModeTransaction); err != nil {
		fmt.Printf("Failed to add PrinterSink3: [+%v]", err)
	}

	return cnv, nil
}
