package conveyor

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers — concrete executor implementations
// ---------------------------------------------------------------------------

// testSource is a minimal SourceExecutor[int] for testing the sourceWrapper.
// Execute returns a fixed value; ExecuteLoop emits that value and value+1.
type testSource struct {
	ConcreteSourceExecutor[int]
	value int
}

func (s *testSource) Execute(ctx CnvContext) (int, error) { return s.value, nil }

func (s *testSource) ExecuteLoop(ctx CnvContext, out chan<- int) error {
	out <- s.value
	out <- s.value + 1
	return nil
}

// testOp is a minimal OperationExecutor[int, string] for testing the operationWrapper.
type testOp struct {
	ConcreteOperationExecutor[int, string]
}

func (o *testOp) Execute(ctx CnvContext, in int) (string, error) {
	return fmt.Sprintf("val_%d", in), nil
}

func (o *testOp) ExecuteLoop(ctx CnvContext, in <-chan int, out chan<- string) error {
	for v := range in {
		out <- fmt.Sprintf("val_%d", v)
	}
	return nil
}

// testSink is a minimal SinkExecutor[string] for testing the sinkWrapper.
// It records every value it receives in a slice protected by a mutex.
type testSink struct {
	ConcreteSinkExecutor[string]
	received []string
	mu       sync.Mutex
}

func (s *testSink) Execute(ctx CnvContext, in string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received = append(s.received, in)
	return nil
}

func (s *testSink) ExecuteLoop(ctx CnvContext, in <-chan string) error {
	for v := range in {
		s.mu.Lock()
		s.received = append(s.received, v)
		s.mu.Unlock()
	}
	return nil
}

// newTestContext creates a valid CnvContext backed by a cancelable context.
// It is used wherever a non-nil CnvContext is required by the untyped wrapper
// methods. Because both the test files and the production code live in
// package conveyor, the unexported cnvContext type is accessible here.
func newTestContext() CnvContext {
	ctx := &cnvContext{
		Context: context.Background(),
		Data: CtxData{
			Name:   "test",
			logs:   make(chan Message, 100),
			status: make(chan string, 100),
		},
	}
	return ctx.WithCancel()
}

// ---------------------------------------------------------------------------
// sourceWrapper type-assertion tests
// ---------------------------------------------------------------------------

func TestSourceWrapper_Types(t *testing.T) {
	src := &testSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		value:                  42,
	}
	w := wrapSource[int](src)

	assert.Nil(t, w.InType(), "source InType must be nil — sources have no input")
	assert.Equal(t, reflect.TypeFor[int](), w.OutType())
	assert.Equal(t, WorkerTypeSource, w.WorkerType())
	assert.Equal(t, "src", w.GetName())
	assert.Equal(t, "src", w.GetUniqueIdentifier())
}

func TestOperationWrapper_Types(t *testing.T) {
	op := &testOp{
		ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"},
	}
	w := wrapOperation[int, string](op)

	assert.Equal(t, reflect.TypeFor[int](), w.InType())
	assert.Equal(t, reflect.TypeFor[string](), w.OutType())
	assert.Equal(t, WorkerTypeOperation, w.WorkerType())
}

func TestSinkWrapper_Types(t *testing.T) {
	snk := &testSink{
		ConcreteSinkExecutor: ConcreteSinkExecutor[string]{Name: "snk"},
	}
	w := wrapSink[string](snk)

	assert.Equal(t, reflect.TypeFor[string](), w.InType())
	assert.Nil(t, w.OutType(), "sink OutType must be nil — sinks produce no output")
	assert.Equal(t, WorkerTypeSink, w.WorkerType())
}

// ---------------------------------------------------------------------------
// executeUntyped delegation tests
// ---------------------------------------------------------------------------

// TestSourceWrapper_executeUntyped verifies that the source wrapper delegates to
// the underlying Execute method and boxes the typed result as any.
func TestSourceWrapper_executeUntyped(t *testing.T) {
	src := &testSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		value:                  42,
	}
	w := wrapSource[int](src)

	ctx := newTestContext()
	out, err := w.executeUntyped(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 42, out.(int))
}

// TestOperationWrapper_executeUntyped verifies that the operation wrapper casts
// the inData any value to the typed input and returns the boxed output.
func TestOperationWrapper_executeUntyped(t *testing.T) {
	op := &testOp{
		ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"},
	}
	w := wrapOperation[int, string](op)

	ctx := newTestContext()
	out, err := w.executeUntyped(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, "val_7", out.(string))
}

// TestSinkWrapper_executeUntyped verifies that the sink wrapper casts inData, calls
// Execute on the underlying executor, and always returns a nil output value.
func TestSinkWrapper_executeUntyped(t *testing.T) {
	snk := &testSink{
		ConcreteSinkExecutor: ConcreteSinkExecutor[string]{Name: "snk"},
	}
	w := wrapSink[string](snk)

	ctx := newTestContext()
	out, err := w.executeUntyped(ctx, "hello")
	require.NoError(t, err)
	assert.Nil(t, out, "sink executeUntyped must return nil output")
	assert.Equal(t, []string{"hello"}, snk.received)
}

// ---------------------------------------------------------------------------
// executeLoopUntyped channel-bridging tests
// ---------------------------------------------------------------------------

// TestSourceWrapper_executeLoopUntyped verifies that the source wrapper bridges the
// typed ExecuteLoop output onto the untyped outChan without losing values.
func TestSourceWrapper_executeLoopUntyped(t *testing.T) {
	src := &testSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		value:                  10,
	}
	w := wrapSource[int](src)

	ctx := newTestContext()
	outChan := make(chan any, 10)

	err := w.executeLoopUntyped(ctx, nil, outChan)
	require.NoError(t, err)

	// executeLoopUntyped must have forwarded all values before returning.
	close(outChan)
	var results []int
	for v := range outChan {
		results = append(results, v.(int))
	}
	assert.Equal(t, []int{10, 11}, results)
}

// TestOperationWrapper_executeLoopUntyped verifies the bidirectional channel bridge:
// untyped inChan → typed execute → untyped outChan.
func TestOperationWrapper_executeLoopUntyped(t *testing.T) {
	op := &testOp{
		ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"},
	}
	w := wrapOperation[int, string](op)

	ctx := newTestContext()
	inChan := make(chan any, 10)
	outChan := make(chan any, 10)

	inChan <- 1
	inChan <- 2
	close(inChan)

	err := w.executeLoopUntyped(ctx, inChan, outChan)
	require.NoError(t, err)

	close(outChan)
	var results []string
	for v := range outChan {
		results = append(results, v.(string))
	}
	assert.Equal(t, []string{"val_1", "val_2"}, results)
}

// TestSinkWrapper_executeLoopUntyped verifies that the sink wrapper bridges the
// untyped inChan to the typed ExecuteLoop and collects all values.
func TestSinkWrapper_executeLoopUntyped(t *testing.T) {
	snk := &testSink{
		ConcreteSinkExecutor: ConcreteSinkExecutor[string]{Name: "snk"},
	}
	w := wrapSink[string](snk)

	ctx := newTestContext()
	inChan := make(chan any, 10)

	inChan <- "a"
	inChan <- "b"
	close(inChan)

	err := w.executeLoopUntyped(ctx, inChan, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, snk.received)
}

// ---------------------------------------------------------------------------
// Delegation tests for Count and CleanUp
// ---------------------------------------------------------------------------

// TestWrapper_DelegatesCleanUp verifies that CleanUp is forwarded to the underlying
// executor. The default ConcreteSourceExecutor.CleanUp is a no-op returning nil.
func TestWrapper_DelegatesCleanUp(t *testing.T) {
	src := &testSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
	}
	w := wrapSource[int](src)
	assert.NoError(t, w.CleanUp())
}

// TestWrapper_DelegatesCount verifies that Count is forwarded to the underlying
// executor. The default implementation returns 1.
func TestWrapper_DelegatesCount(t *testing.T) {
	src := &testSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
	}
	w := wrapSource[int](src)
	assert.Equal(t, 1, w.Count())
}

// TestOperationWrapper_DelegatesCount verifies Count delegation for operations.
func TestOperationWrapper_DelegatesCount(t *testing.T) {
	op := &testOp{
		ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"},
	}
	w := wrapOperation[int, string](op)
	assert.Equal(t, 1, w.Count())
}

// TestSinkWrapper_DelegatesCleanUp verifies CleanUp delegation for sinks.
func TestSinkWrapper_DelegatesCleanUp(t *testing.T) {
	snk := &testSink{
		ConcreteSinkExecutor: ConcreteSinkExecutor[string]{Name: "snk"},
	}
	w := wrapSink[string](snk)
	assert.NoError(t, w.CleanUp())
}

// TestOperationWrapper_executeUntyped_MultipleValues verifies correct boxing of
// several different input values in succession.
func TestOperationWrapper_executeUntyped_MultipleValues(t *testing.T) {
	op := &testOp{
		ConcreteOperationExecutor: ConcreteOperationExecutor[int, string]{Name: "op"},
	}
	w := wrapOperation[int, string](op)
	ctx := newTestContext()

	cases := []struct {
		in  int
		out string
	}{
		{0, "val_0"},
		{99, "val_99"},
		{-1, "val_-1"},
	}
	for _, tc := range cases {
		out, err := w.executeUntyped(ctx, tc.in)
		require.NoError(t, err)
		assert.Equal(t, tc.out, out.(string))
	}
}

// ---------------------------------------------------------------------------
// Concrete base struct default tests
// ---------------------------------------------------------------------------

func TestConcreteSourceExecutor_Defaults(t *testing.T) {
	src := &ConcreteSourceExecutor[int]{Name: "test_source", Data: "some_data"}
	assert.Equal(t, "test_source", src.GetName())
	assert.Equal(t, "test_source", src.GetUniqueIdentifier())
	assert.Equal(t, 1, src.Count())
	assert.NoError(t, src.CleanUp())

	val, err := src.Execute(nil)
	assert.Equal(t, 0, val)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)

	err = src.ExecuteLoop(nil, nil)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)
}

func TestConcreteOperationExecutor_Defaults(t *testing.T) {
	op := &ConcreteOperationExecutor[string, int]{Name: "test_op"}
	assert.Equal(t, "test_op", op.GetName())
	assert.Equal(t, "test_op", op.GetUniqueIdentifier())
	assert.Equal(t, 1, op.Count())
	assert.NoError(t, op.CleanUp())

	val, err := op.Execute(nil, "input")
	assert.Equal(t, 0, val)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)

	err = op.ExecuteLoop(nil, nil, nil)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)
}

func TestConcreteSinkExecutor_Defaults(t *testing.T) {
	snk := &ConcreteSinkExecutor[float64]{Name: "test_sink"}
	assert.Equal(t, "test_sink", snk.GetName())
	assert.Equal(t, 1, snk.Count())
	assert.NoError(t, snk.CleanUp())

	err := snk.Execute(nil, 3.14)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)

	err = snk.ExecuteLoop(nil, nil)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)
}

func TestConcreteJointExecutor_Defaults(t *testing.T) {
	jnt := &ConcreteJointExecutor[int, int]{Name: "test_joint"}
	assert.Equal(t, "test_joint", jnt.GetName())
	assert.Equal(t, "test_joint", jnt.GetUniqueIdentifier())
	assert.Equal(t, 1, jnt.Count())
	assert.Equal(t, 1, jnt.InputCount())
	assert.Equal(t, 1, jnt.OutputCount())

	err := jnt.ExecuteLoop(nil, nil, nil)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)
}

// TestConcreteSourceExecutor_ZeroValue_String verifies that the default Execute
// implementation returns the zero value of the type parameter (empty string for string).
func TestConcreteSourceExecutor_ZeroValue_String(t *testing.T) {
	src := &ConcreteSourceExecutor[string]{Name: "str_source"}
	val, err := src.Execute(nil)
	assert.Equal(t, "", val)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)
}

// TestConcreteOperationExecutor_ZeroValue_Struct verifies that the default Execute
// implementation returns the zero value of a struct type parameter.
func TestConcreteOperationExecutor_ZeroValue_Struct(t *testing.T) {
	type MyStruct struct{ X int }
	op := &ConcreteOperationExecutor[int, MyStruct]{Name: "struct_op"}
	val, err := op.Execute(nil, 42)
	assert.Equal(t, MyStruct{}, val)
	assert.ErrorIs(t, err, ErrExecuteNotImplemented)
}

// TestConcreteSourceExecutor_Data verifies that the Data field is stored and
// accessible after construction.
func TestConcreteSourceExecutor_Data(t *testing.T) {
	src := &ConcreteSourceExecutor[int]{Name: "src", Data: 42}
	assert.Equal(t, 42, src.Data)
}

// TestConcreteOperationExecutor_Data verifies the Data field on operation executors.
func TestConcreteOperationExecutor_Data(t *testing.T) {
	op := &ConcreteOperationExecutor[int, string]{Name: "op", Data: "metadata"}
	assert.Equal(t, "metadata", op.Data)
}

// TestConcreteSinkExecutor_GetUniqueIdentifier verifies that GetUniqueIdentifier
// returns the same value as GetName for the sink base struct.
func TestConcreteSinkExecutor_GetUniqueIdentifier(t *testing.T) {
	snk := &ConcreteSinkExecutor[int]{Name: "my_sink"}
	assert.Equal(t, snk.GetName(), snk.GetUniqueIdentifier())
}

// TestConcreteJointExecutor_DifferentTypes verifies that the joint base
// struct compiles and behaves correctly with heterogeneous type parameters.
func TestConcreteJointExecutor_DifferentTypes(t *testing.T) {
	jnt := &ConcreteJointExecutor[string, float64]{Name: "transform_joint"}
	assert.Equal(t, "transform_joint", jnt.GetName())
	assert.Equal(t, 1, jnt.InputCount())
	assert.Equal(t, 1, jnt.OutputCount())
}
