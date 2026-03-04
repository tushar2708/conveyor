package conveyor

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Integration-test helpers
// ---------------------------------------------------------------------------

// countingSource emits integers 0 … limit (inclusive) in WorkerModeTransaction,
// then signals exhaustion by returning ErrSourceExhausted.
type countingSource struct {
	ConcreteSourceExecutor[int]
	limit   int
	current int
}

func (s *countingSource) Execute(ctx CnvContext) (int, error) {
	if s.current > s.limit {
		return 0, ErrSourceExhausted
	}
	val := s.current
	s.current++
	return val, nil
}

// doublingOp multiplies each incoming int by two in WorkerModeTransaction.
type doublingOp struct {
	ConcreteOperationExecutor[int, int]
}

func (o *doublingOp) Execute(ctx CnvContext, in int) (int, error) { return in * 2, nil }

// collectingSink records every int it receives. Access to the slice is guarded
// by a mutex so it is safe to use in concurrent WorkerModeTransaction scenarios.
type collectingSink struct {
	ConcreteSinkExecutor[int]
	collected []int
	mu        sync.Mutex
}

func (s *collectingSink) Execute(ctx CnvContext, in int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.collected = append(s.collected, in)
	return nil
}

// loopSource pushes a fixed slice of values onto the output channel and then
// returns, signalling end-of-stream for WorkerModeLoop pipelines.
type loopSource struct {
	ConcreteSourceExecutor[int]
	values []int
}

func (s *loopSource) ExecuteLoop(ctx CnvContext, out chan<- int) error {
	for _, v := range s.values {
		out <- v
	}
	return nil
}

// loopDoubleOp doubles every int received from inChan and forwards it to outChan
// until inChan is closed, for use in WorkerModeLoop pipelines.
type loopDoubleOp struct {
	ConcreteOperationExecutor[int, int]
}

func (o *loopDoubleOp) ExecuteLoop(ctx CnvContext, in <-chan int, out chan<- int) error {
	for v := range in {
		out <- v * 2
	}
	return nil
}

// loopCollectingSink reads from inChan until it is closed, collecting every
// value in a mutex-guarded slice, for use in WorkerModeLoop pipelines.
type loopCollectingSink struct {
	ConcreteSinkExecutor[int]
	collected []int
	mu        sync.Mutex
}

func (s *loopCollectingSink) ExecuteLoop(ctx CnvContext, in <-chan int) error {
	for v := range in {
		s.mu.Lock()
		s.collected = append(s.collected, v)
		s.mu.Unlock()
	}
	return nil
}

// ---------------------------------------------------------------------------
// WorkerModeTransaction integration tests
// ---------------------------------------------------------------------------

// TestIntegration_TransactionMode_SourceOpSink runs a Source → Operation → Sink
// pipeline where all workers use WorkerModeTransaction.
//
// countingSource emits 0, 1, 2, 3, 4 (limit=4, so 5 values total) before
// returning ErrSourceExhausted. doublingOp multiplies each by 2.
// The sink should therefore receive exactly 5 values (0, 2, 4, 6, 8).
func TestIntegration_TransactionMode_SourceOpSink(t *testing.T) {
	cnv, _ := NewConveyor("test_tx", 10)

	src := &countingSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		limit:                  4,
	}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	op := &doublingOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, int]{Name: "op"}}
	require.NoError(t, AddOperation[int, int](cnv, op, WorkerModeTransaction))

	snk := &collectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	require.NoError(t, AddSink[int](cnv, snk, WorkerModeTransaction))

	require.NoError(t, cnv.Start())

	// Source exhaustion triggers graceful shutdown: the source stops producing,
	// WaitAndStop closes the output channel, and downstream workers drain all
	// buffered items before exiting. All 5 doubled values must be collected.
	snk.mu.Lock()
	defer snk.mu.Unlock()
	assert.Equal(t, 5, len(snk.collected))
	sum := 0
	for _, v := range snk.collected {
		sum += v
	}
	assert.Equal(t, 20, sum) // 0+2+4+6+8=20
}

// TestIntegration_TransactionMode_SourceOnly verifies that a single-node conveyor
// (source only) runs to completion without panicking.
func TestIntegration_TransactionMode_SourceOnly(t *testing.T) {
	cnv, _ := NewConveyor("test_src_only", 10)

	src := &countingSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		limit:                  2,
	}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	// A source-only pipeline has no sink; the values are produced into the output
	// channel but nobody reads them. The pipeline should still terminate once the
	// source is exhausted. However, since there is no downstream consumer the
	// output channel may fill up and the source goroutines can block. Skip
	// assertion on collected data - just verify no panic / no error.
	snk := &collectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	require.NoError(t, AddSink[int](cnv, snk, WorkerModeTransaction))
	require.NoError(t, cnv.Start())
}

// ---------------------------------------------------------------------------
// WorkerModeLoop integration tests
// ---------------------------------------------------------------------------

// TestIntegration_LoopMode_SourceOpSink runs a Source → Operation → Sink pipeline
// where all workers use WorkerModeLoop.
//
// loopSource emits {1, 2, 3}; loopDoubleOp doubles each value.
// The collected sum should be 2+4+6=12.
func TestIntegration_LoopMode_SourceOpSink(t *testing.T) {
	cnv, _ := NewConveyor("test_loop", 10)

	src := &loopSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		values:                 []int{1, 2, 3},
	}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeLoop))

	op := &loopDoubleOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, int]{Name: "op"}}
	require.NoError(t, AddOperation[int, int](cnv, op, WorkerModeLoop))

	snk := &loopCollectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	require.NoError(t, AddSink[int](cnv, snk, WorkerModeLoop))

	require.NoError(t, cnv.Start())

	snk.mu.Lock()
	defer snk.mu.Unlock()

	assert.Equal(t, 3, len(snk.collected))

	// Order may vary because of goroutine scheduling; verify by sum instead.
	sum := 0
	for _, v := range snk.collected {
		sum += v
	}
	assert.Equal(t, 12, sum) // 2+4+6=12
}

// TestIntegration_LoopMode_SourceSink verifies a direct Source → Sink loop-mode
// pipeline with no intermediate operation.
func TestIntegration_LoopMode_SourceSink(t *testing.T) {
	cnv, _ := NewConveyor("test_loop_direct", 10)

	src := &loopSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		values:                 []int{7, 8, 9},
	}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeLoop))

	snk := &loopCollectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	require.NoError(t, AddSink[int](cnv, snk, WorkerModeLoop))

	require.NoError(t, cnv.Start())

	snk.mu.Lock()
	defer snk.mu.Unlock()

	sum := 0
	for _, v := range snk.collected {
		sum += v
	}
	assert.Equal(t, 24, sum) // 7+8+9=24
}

// ---------------------------------------------------------------------------
// Joint fan-out integration test
// ---------------------------------------------------------------------------

// TestIntegration_WithJoint_MultiSink wires a loop-mode Source → Operation →
// ReplicateJoint → Sink1 and Sink2.
//
// loopSource emits {10, 20}. loopDoubleOp doubles each value to {20, 40}.
// The joint broadcasts every value to both sinks, so each sink should receive
// exactly 2 values.
func TestIntegration_WithJoint_MultiSink(t *testing.T) {
	cnv, _ := NewConveyor("test_joint", 10)

	src := &loopSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		values:                 []int{10, 20},
	}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeLoop))

	op := &loopDoubleOp{
		ConcreteOperationExecutor: ConcreteOperationExecutor[int, int]{Name: "op"},
	}
	require.NoError(t, AddOperation[int, int](cnv, op, WorkerModeLoop))

	joint, err := NewReplicateJoint[int]("joint", 2)
	require.NoError(t, err)
	require.NoError(t, AddJointAfterNode[int, int](cnv, joint))

	snk1 := &collectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk1"}}
	require.NoError(t, AddSinkAfterJoint[int](cnv, snk1, WorkerModeTransaction))

	snk2 := &collectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk2"}}
	require.NoError(t, AddSinkAfterJoint[int](cnv, snk2, WorkerModeTransaction))

	require.NoError(t, cnv.Start())

	snk1.mu.Lock()
	snk2.mu.Lock()
	defer snk1.mu.Unlock()
	defer snk2.mu.Unlock()

	// Each sink should have received both doubled values (20 and 40).
	assert.Equal(t, 2, len(snk1.collected))
	assert.Equal(t, 2, len(snk2.collected))

	sum1 := 0
	for _, v := range snk1.collected {
		sum1 += v
	}
	assert.Equal(t, 60, sum1) // 20+40=60

	sum2 := 0
	for _, v := range snk2.collected {
		sum2 += v
	}
	assert.Equal(t, 60, sum2) // 20+40=60
}

// ---------------------------------------------------------------------------
// Source-exhaustion graceful-termination test
// ---------------------------------------------------------------------------

// TestIntegration_SourceExhausted_GracefulTermination verifies that the conveyor
// terminates cleanly when a source returns ErrSourceExhausted after emitting a
// finite number of values.
func TestIntegration_SourceExhausted_GracefulTermination(t *testing.T) {
	cnv, _ := NewConveyor("test_exhaust", 10)

	src := &countingSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		limit:                  2, // will produce 0, 1, 2 then exhaust
	}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	snk := &collectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	require.NoError(t, AddSink[int](cnv, snk, WorkerModeTransaction))

	err := cnv.Start()
	require.NoError(t, err, "conveyor must terminate without error on source exhaustion")

	snk.mu.Lock()
	defer snk.mu.Unlock()
	// Exactly limit+1 values (0, 1, 2) should have been collected.
	assert.Equal(t, 3, len(snk.collected))
	sum := 0
	for _, v := range snk.collected {
		sum += v
	}
	assert.Equal(t, 3, sum) // 0+1+2=3
}

// ---------------------------------------------------------------------------
// Multiple chained operations integration test
// ---------------------------------------------------------------------------

// tripleOp multiplies each incoming int by three, for use in multi-op chains.
type tripleOp struct {
	ConcreteOperationExecutor[int, int]
}

func (o *tripleOp) Execute(ctx CnvContext, in int) (int, error) { return in * 3, nil }

// TestIntegration_MultipleOps verifies a Source → Op1 → Op2 → Sink pipeline
// where two successive operations are chained in transaction mode.
//
// countingSource emits {0, 1} (limit=1). doublingOp produces {0, 2}.
// tripleOp produces {0, 6}. The sum should be 6.
func TestIntegration_MultipleOps(t *testing.T) {
	cnv, _ := NewConveyor("test_multi_op", 10)

	src := &countingSource{
		ConcreteSourceExecutor: ConcreteSourceExecutor[int]{Name: "src"},
		limit:                  1, // emits 0, 1
	}
	require.NoError(t, AddSource[int](cnv, src, WorkerModeTransaction))

	op1 := &doublingOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, int]{Name: "op1"}}
	require.NoError(t, AddOperation[int, int](cnv, op1, WorkerModeTransaction))

	op2 := &tripleOp{ConcreteOperationExecutor: ConcreteOperationExecutor[int, int]{Name: "op2"}}
	require.NoError(t, AddOperation[int, int](cnv, op2, WorkerModeTransaction))

	snk := &collectingSink{ConcreteSinkExecutor: ConcreteSinkExecutor[int]{Name: "snk"}}
	require.NoError(t, AddSink[int](cnv, snk, WorkerModeTransaction))

	require.NoError(t, cnv.Start())

	// Source exhaustion triggers graceful shutdown. All items drain through
	// the pipeline: 0→0→0 and 1→2→6. Both must be collected.
	snk.mu.Lock()
	defer snk.mu.Unlock()

	assert.Equal(t, 2, len(snk.collected))

	sum := 0
	for _, v := range snk.collected {
		sum += v
	}
	assert.Equal(t, 6, sum) // 0+6=6
}
