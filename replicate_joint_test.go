package conveyor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNewReplicateJoint(t *testing.T) {
	rj, err := NewReplicateJoint[int]("test_joint", 3)
	require.NoError(t, err)
	assert.Equal(t, "test_joint", rj.GetName())
	assert.Equal(t, "test_joint", rj.GetUniqueIdentifier())
	assert.Equal(t, 1, rj.Count())
	assert.Equal(t, 1, rj.InputCount())
	assert.Equal(t, 3, rj.OutputCount())
}

// TestNewReplicateJoint_OutChanCountStored verifies that the requested
// number of output channels is stored and later reported by OutputCount.
func TestNewReplicateJoint_OutChanCountStored(t *testing.T) {
	for _, n := range []int{2, 3, 5, 10} {
		rj, err := NewReplicateJoint[string]("rj", n)
		require.NoError(t, err)
		assert.Equal(t, n, rj.OutputCount())
	}
}

// ---------------------------------------------------------------------------
// ExecuteLoop happy-path tests
// ---------------------------------------------------------------------------

// TestReplicateJoint_ExecuteLoop_Success verifies that every value
// received on the single input channel is broadcast to all output channels.
func TestReplicateJoint_ExecuteLoop_Success(t *testing.T) {
	rj, _ := NewReplicateJoint[int]("rj", 2)

	inChan := make(chan int, 3)
	outChan1 := make(chan int, 3)
	outChan2 := make(chan int, 3)

	inChan <- 1
	inChan <- 2
	inChan <- 3
	close(inChan)

	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan int{inChan}, []chan int{outChan1, outChan2})
	require.NoError(t, err)

	// Both output channels must contain all three values in order.
	assert.Equal(t, 3, len(outChan1))
	assert.Equal(t, 3, len(outChan2))

	assert.Equal(t, 1, <-outChan1)
	assert.Equal(t, 2, <-outChan1)
	assert.Equal(t, 3, <-outChan1)

	assert.Equal(t, 1, <-outChan2)
	assert.Equal(t, 2, <-outChan2)
	assert.Equal(t, 3, <-outChan2)
}

// TestReplicateJoint_ExecuteLoop_ThreeOutputs verifies fan-out to three
// output channels simultaneously.
func TestReplicateJoint_ExecuteLoop_ThreeOutputs(t *testing.T) {
	rj, _ := NewReplicateJoint[int]("rj", 3)

	inChan := make(chan int, 2)
	out1 := make(chan int, 2)
	out2 := make(chan int, 2)
	out3 := make(chan int, 2)

	inChan <- 10
	inChan <- 20
	close(inChan)

	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan int{inChan}, []chan int{out1, out2, out3})
	require.NoError(t, err)

	for _, ch := range []chan int{out1, out2, out3} {
		assert.Equal(t, 10, <-ch)
		assert.Equal(t, 20, <-ch)
	}
}

// TestReplicateJoint_ExecuteLoop_EmptyInput verifies that the joint
// returns immediately and writes nothing when the input channel is closed before
// any values are sent.
func TestReplicateJoint_ExecuteLoop_EmptyInput(t *testing.T) {
	rj, _ := NewReplicateJoint[int]("rj", 2)

	inChan := make(chan int)
	close(inChan) // nothing to send

	out1 := make(chan int, 1)
	out2 := make(chan int, 1)

	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan int{inChan}, []chan int{out1, out2})
	require.NoError(t, err)

	assert.Equal(t, 0, len(out1), "no values should have been forwarded")
	assert.Equal(t, 0, len(out2), "no values should have been forwarded")
}

// ---------------------------------------------------------------------------
// ExecuteLoop error-guard tests
// ---------------------------------------------------------------------------

func TestReplicateJoint_NoInput(t *testing.T) {
	rj, _ := NewReplicateJoint[int]("rj", 2)
	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan int{}, []chan int{make(chan int)})
	assert.ErrorIs(t, err, ErrNoInputChannel)
}

func TestReplicateJoint_NoOutput(t *testing.T) {
	rj, _ := NewReplicateJoint[int]("rj", 2)
	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan int{make(chan int)}, []chan int{})
	assert.ErrorIs(t, err, ErrNoOutputChannel)
}

func TestReplicateJoint_MultipleInputs(t *testing.T) {
	rj, _ := NewReplicateJoint[int]("rj", 2)
	ctx := newTestContext()
	err := rj.ExecuteLoop(
		ctx,
		[]chan int{make(chan int), make(chan int)},
		[]chan int{make(chan int)},
	)
	assert.ErrorIs(t, err, ErrMultipleInputChannels)
}

// TestReplicateJoint_OneToOne verifies that a 1-to-1 configuration is
// rejected because the replicate joint is unnecessary in that topology.
func TestReplicateJoint_OneToOne(t *testing.T) {
	rj, _ := NewReplicateJoint[int]("rj", 1)
	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan int{make(chan int)}, []chan int{make(chan int)})
	assert.ErrorIs(t, err, ErrOneToOneConnection)
}

// ---------------------------------------------------------------------------
// Type-parameterisation tests
// ---------------------------------------------------------------------------

// TestReplicateJoint_StringType verifies that the joint works correctly
// when the type parameter is string.
func TestReplicateJoint_StringType(t *testing.T) {
	rj, _ := NewReplicateJoint[string]("rj", 2)

	inChan := make(chan string, 2)
	outChan1 := make(chan string, 2)
	outChan2 := make(chan string, 2)

	inChan <- "hello"
	inChan <- "world"
	close(inChan)

	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan string{inChan}, []chan string{outChan1, outChan2})
	require.NoError(t, err)

	assert.Equal(t, "hello", <-outChan1)
	assert.Equal(t, "world", <-outChan1)
	assert.Equal(t, "hello", <-outChan2)
	assert.Equal(t, "world", <-outChan2)
}

// TestReplicateJoint_StructType verifies that the joint correctly copies
// struct values (not pointers) so that each output channel receives an
// independent copy of the same value.
func TestReplicateJoint_StructType(t *testing.T) {
	type Record struct {
		ID   int
		Name string
	}

	rj, _ := NewReplicateJoint[Record]("rj", 2)

	inChan := make(chan Record, 1)
	outChan1 := make(chan Record, 1)
	outChan2 := make(chan Record, 1)

	inChan <- Record{ID: 1, Name: "test"}
	close(inChan)

	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan Record{inChan}, []chan Record{outChan1, outChan2})
	require.NoError(t, err)

	r1 := <-outChan1
	r2 := <-outChan2
	assert.Equal(t, Record{ID: 1, Name: "test"}, r1)
	assert.Equal(t, Record{ID: 1, Name: "test"}, r2)
}

// TestReplicateJoint_PointerType verifies that the joint works correctly
// with pointer values; both outputs receive the same pointer.
func TestReplicateJoint_PointerType(t *testing.T) {
	type Record struct{ ID int }

	rj, _ := NewReplicateJoint[*Record]("rj", 2)

	rec := &Record{ID: 99}
	inChan := make(chan *Record, 1)
	out1 := make(chan *Record, 1)
	out2 := make(chan *Record, 1)

	inChan <- rec
	close(inChan)

	ctx := newTestContext()
	err := rj.ExecuteLoop(ctx, []chan *Record{inChan}, []chan *Record{out1, out2})
	require.NoError(t, err)

	assert.Equal(t, rec, <-out1)
	assert.Equal(t, rec, <-out2)
}
