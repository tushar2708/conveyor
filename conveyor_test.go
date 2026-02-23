package conveyor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Minimal executor implementations used only for conveyor-construction tests.
// These do not need to run inside a live pipeline; they only need to satisfy
// the generic interface constraints so that the type-checking logic inside
// AddSource / AddOperation / AddSink / Add*AfterJoint can be exercised.
// ---------------------------------------------------------------------------

type intSource struct {
	ConcreteSourceExecutor[int]
}

func (s *intSource) Execute(ctx CnvContext) (int, error) { return 0, ErrSourceExhausted }

type intToStringOp struct {
	ConcreteOperationExecutor[int, string]
}

func (o *intToStringOp) Execute(ctx CnvContext, in int) (string, error) { return "ok", nil }

type stringToFloatOp struct {
	ConcreteOperationExecutor[string, float64]
}

func (o *stringToFloatOp) Execute(ctx CnvContext, in string) (float64, error) { return 1.0, nil }

type floatSink struct {
	ConcreteSinkExecutor[float64]
}

func (s *floatSink) Execute(ctx CnvContext, in float64) error { return nil }

type stringSink struct {
	ConcreteSinkExecutor[string]
}

func (s *stringSink) Execute(ctx CnvContext, in string) error { return nil }

type intSink struct {
	ConcreteSinkExecutor[int]
}

func (s *intSink) Execute(ctx CnvContext, in int) error { return nil }

type stringToStringOp struct {
	ConcreteOperationExecutor[string, string]
}

func (o *stringToStringOp) Execute(ctx CnvContext, in string) (string, error) { return in, nil }

// ---------------------------------------------------------------------------
// AddSource tests
// ---------------------------------------------------------------------------

func TestAddSource_Success(t *testing.T) {
	cnv, err := NewConveyor("test", 10)
	require.NoError(t, err)

	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))
	assert.Equal(t, 1, len(cnv.workers))
}

// TestAddSource_SetsLastNodeOutType verifies that after AddSource the conveyor
// records the output type so the next node can validate its input type.
func TestAddSource_SetsLastNodeOutType(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	// lastNodeOutType must be reflect.TypeFor[int]() after adding an int source.
	assert.NotNil(t, cnv.lastNodeOutType)
}

// ---------------------------------------------------------------------------
// AddOperation tests
// ---------------------------------------------------------------------------

func TestAddOperation_TypeMatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	op := &intToStringOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"}}
	err := AddOperation[int, string](cnv, op, WorkerModeTransaction)
	require.NoError(t, err)
	assert.Equal(t, 2, len(cnv.workers))
}

func TestAddOperation_TypeMismatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	// Source outputs int but the operation expects string — must fail.
	op := &stringToStringOp{ConcreteOperationExecutor: ConcreteOperationExecutor[string, string]{Name: "op"}}
	err := AddOperation[string, string](cnv, op, WorkerModeTransaction)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrTypeMismatch))
}

// ---------------------------------------------------------------------------
// AddSink tests
// ---------------------------------------------------------------------------

func TestAddSink_TypeMatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	snk := &intSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	err := AddSink[int](cnv, snk, WorkerModeTransaction)
	require.NoError(t, err)
}

func TestAddSink_TypeMismatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	// Source outputs int but the sink expects string — must fail.
	snk := &stringSink{ConcreteSinkExecutor: ConcreteSinkExecutor[string]{Name: "snk"}}
	err := AddSink[string](cnv, snk, WorkerModeTransaction)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrTypeMismatch))
}

// TestAddSink_ClearsLastNodeOutType verifies that after a sink is successfully
// added, the conveyor clears lastNodeOutType because sinks produce no output.
func TestAddSink_ClearsLastNodeOutType(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	snk := &intSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	require.NoError(t, AddSink[int](cnv, snk, WorkerModeTransaction))

	assert.Nil(t, cnv.lastNodeOutType, "lastNodeOutType must be nil after a sink is added")
}

// ---------------------------------------------------------------------------
// Chained type-compatibility tests
// ---------------------------------------------------------------------------

func TestChainedTypes_Success(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)

	// int → string → float64 — all adjacent types must agree.
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	op1 := &intToStringOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op1"}}
	require.NoError(t, AddOperation[int, string](cnv, op1, WorkerModeTransaction))

	op2 := &stringToFloatOp{ConcreteOperationExecutor: ConcreteOperationExecutor[string, float64]{Name: "op2"}}
	require.NoError(t, AddOperation[string, float64](cnv, op2, WorkerModeTransaction))

	snk := &floatSink{ConcreteSinkExecutor: ConcreteSinkExecutor[float64]{Name: "snk"}}
	require.NoError(t, AddSink[float64](cnv, snk, WorkerModeTransaction))

	assert.Equal(t, 4, len(cnv.workers))
}

func TestChainedTypes_MismatchInMiddle(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)

	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	op1 := &intToStringOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op1"}}
	require.NoError(t, AddOperation[int, string](cnv, op1, WorkerModeTransaction))

	// op1 emits string but floatSink expects float64 — must fail.
	snk := &floatSink{ConcreteSinkExecutor: ConcreteSinkExecutor[float64]{Name: "snk"}}
	err := AddSink[float64](cnv, snk, WorkerModeTransaction)
	assert.True(t, errors.Is(err, ErrTypeMismatch))
}

// ---------------------------------------------------------------------------
// AddJointAfterNode tests
// ---------------------------------------------------------------------------

func TestAddJointAfterNode_TypeMatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	joint, err := NewReplicateJoint[int]("joint", 2)
	require.NoError(t, err)

	require.NoError(t, AddJointAfterNode[int, int](cnv, joint))
	assert.Equal(t, 1, len(cnv.joints))
}

func TestAddJointAfterNode_TypeMismatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	// Source outputs int but the joint expects string input — must fail.
	joint, _ := NewReplicateJoint[string]("joint", 2)
	err := AddJointAfterNode[string, string](cnv, joint)
	assert.True(t, errors.Is(err, ErrTypeMismatch))
}

func TestAddJointAfterNode_NoNodes(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	joint, _ := NewReplicateJoint[int]("joint", 2)
	err := AddJointAfterNode[int, int](cnv, joint)
	assert.ErrorIs(t, err, ErrNoNodesAvailable)
}

// TestAddJointAfterNode_ClearsLastNodeOutType verifies that after a joint is
// successfully added, lastNodeOutType is cleared because subsequent nodes must
// be added via AddSinkAfterJoint or AddOperationAfterJoint.
func TestAddJointAfterNode_ClearsLastNodeOutType(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	joint, _ := NewReplicateJoint[int]("joint", 2)
	require.NoError(t, AddJointAfterNode[int, int](cnv, joint))

	assert.Nil(t, cnv.lastNodeOutType, "lastNodeOutType must be nil after a joint is installed")
}

// ---------------------------------------------------------------------------
// AddSinkAfterJoint tests
// ---------------------------------------------------------------------------

func TestAddSinkAfterJoint_TypeMatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	joint, _ := NewReplicateJoint[int]("joint", 2)
	require.NoError(t, AddJointAfterNode[int, int](cnv, joint))

	snk := &intSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	err := AddSinkAfterJoint[int](cnv, snk, WorkerModeTransaction)
	require.NoError(t, err)
}

func TestAddSinkAfterJoint_TypeMismatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	joint, _ := NewReplicateJoint[int]("joint", 2)
	require.NoError(t, AddJointAfterNode[int, int](cnv, joint))

	// Joint outputs int but the sink expects string — must fail.
	snk := &stringSink{ConcreteSinkExecutor: ConcreteSinkExecutor[string]{Name: "snk"}}
	err := AddSinkAfterJoint[string](cnv, snk, WorkerModeTransaction)
	assert.True(t, errors.Is(err, ErrTypeMismatch))
}

func TestAddSinkAfterJoint_NoJoints(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	snk := &intSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	err := AddSinkAfterJoint[int](cnv, snk, WorkerModeTransaction)
	assert.ErrorIs(t, err, ErrNoJointsAvailable)
}

// ---------------------------------------------------------------------------
// AddOperationAfterJoint tests
// ---------------------------------------------------------------------------

func TestAddOperationAfterJoint_TypeMatch(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	joint, _ := NewReplicateJoint[int]("joint", 2)
	require.NoError(t, AddJointAfterNode[int, int](cnv, joint))

	op := &intToStringOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"}}
	err := AddOperationAfterJoint[int, string](cnv, op, WorkerModeTransaction)
	require.NoError(t, err)
}

func TestAddOperationAfterJoint_NoJoints(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	op := &intToStringOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"}}
	err := AddOperationAfterJoint[int, string](cnv, op, WorkerModeTransaction)
	assert.ErrorIs(t, err, ErrNoJointsAvailable)
}

// ---------------------------------------------------------------------------
// Struct and pointer type tests
// ---------------------------------------------------------------------------

// TestAddOperation_StructTypes verifies that the generic type-checking machinery
// works correctly when the pipeline carries user-defined struct values.
func TestAddOperation_StructTypes(t *testing.T) {
	type InputData struct{ ID int }
	type OutputData struct{ Name string }

	cnv, _ := NewConveyor("test", 10)

	type structSource struct {
		ConcreteSourceExecutor[InputData]
	}
	src := &structSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[InputData]{Name: "src"},
	}
	require.NoError(t, AddSource[InputData](cnv, src, WorkerModeTransaction))

	type structOp struct {
		ConcreteOperationExecutor[InputData, OutputData]
	}
	op := &structOp{
		ConcreteOperationExecutor: ConcreteOperationExecutor[InputData, OutputData]{Name: "op"},
	}
	require.NoError(t, AddOperation[InputData, OutputData](cnv, op, WorkerModeTransaction))
}

// TestAddOperation_PointerTypes verifies that the generic type-checking machinery
// works correctly when the pipeline carries pointer values.
func TestAddOperation_PointerTypes(t *testing.T) {
	type Record struct{ ID int }

	cnv, _ := NewConveyor("test", 10)

	type ptrSource struct {
		ConcreteSourceExecutor[*Record]
	}
	src := &ptrSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[*Record]{Name: "src"},
	}
	require.NoError(t, AddSource[*Record](cnv, src, WorkerModeTransaction))

	type ptrSink struct {
		ConcreteSinkExecutor[*Record]
	}
	snk := &ptrSink{
		ConcreteSinkExecutor: ConcreteSinkExecutor[*Record]{Name: "snk"},
	}
	require.NoError(t, AddSink[*Record](cnv, snk, WorkerModeTransaction))
}

// ---------------------------------------------------------------------------
// Error-message quality tests
// ---------------------------------------------------------------------------

// TestTypeMismatch_ContainsTypeNames verifies that ErrTypeMismatch error messages
// include the names of both the expected and actual types so that callers can
// quickly diagnose which types are in conflict.
func TestTypeMismatch_ContainsTypeNames(t *testing.T) {
	cnv, _ := NewConveyor("test", 10)
	src := &intSource{ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"}}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	snk := &stringSink{ConcreteSinkExecutor: ConcreteSinkExecutor[string]{Name: "snk"}}
	err := AddSink[string](cnv, snk, WorkerModeTransaction)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "int")
	assert.Contains(t, err.Error(), "string")
}
