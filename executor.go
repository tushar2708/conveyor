package conveyor

import (
	"reflect"
	"sync"
)

// ---------------------------------------------------------------------------
// Internal type-erased interfaces
// ---------------------------------------------------------------------------

// nodeExecutor is the type-erased internal interface used by worker pools.
// It bridges the generic public executor API to the untyped chan any infrastructure
// that worker pools use internally. Each concrete node executor kind (source,
// operation, sink) is wrapped by a dedicated adapter that implements this interface.
type nodeExecutor interface {
	GetName() string
	GetUniqueIdentifier() string

	// InType returns the reflect.Type of the input value. Sources return nil because
	// they produce data without consuming any input.
	InType() reflect.Type

	// OutType returns the reflect.Type of the output value. Sinks return nil because
	// they consume data without producing any output.
	OutType() reflect.Type

	// WorkerType returns one of WorkerTypeSource, WorkerTypeOperation, or WorkerTypeSink.
	WorkerType() string

	// executeUntyped invokes the underlying executor's single-item Execute method.
	// inData is ignored for sources. Returns the output value boxed as any, or nil
	// for sinks.
	executeUntyped(ctx CnvContext, inData any) (any, error)

	// executeLoopUntyped invokes the underlying executor's stream-oriented ExecuteLoop
	// method, bridging the untyped any channels to the typed channels that the
	// wrapped executor expects.
	executeLoopUntyped(ctx CnvContext, inChan <-chan any, outChan chan<- any) error

	Count() int
	CleanUp() error
}

// jointExecutor is the type-erased internal interface used by joint worker
// pools. Joint executors operate on slices of channels, so the erased form carries
// slices of chan any that are bridged to the typed channel slices expected by the
// underlying generic executor.
type jointExecutor interface {
	GetName() string
	GetUniqueIdentifier() string

	// InType returns the reflect.Type of values flowing in through the input channels.
	InType() reflect.Type

	// OutType returns the reflect.Type of values flowing out through the output channels.
	OutType() reflect.Type

	// executeLoopUntyped bridges slices of untyped chan any to the typed channel
	// slices expected by the wrapped JointExecutor.
	executeLoopUntyped(ctx CnvContext, inChans []chan any, outChans []chan any) error

	Count() int
	InputCount() int
	OutputCount() int
}

// ---------------------------------------------------------------------------
// Public generic interfaces
// ---------------------------------------------------------------------------

// SourceExecutor is the generic public interface for source nodes. Implementations
// produce values of type TOut either on demand (Execute) or in a continuous stream
// (ExecuteLoop).
type SourceExecutor[TOut any] interface {
	GetName() string
	GetUniqueIdentifier() string
	Execute(ctx CnvContext) (TOut, error)
	ExecuteLoop(ctx CnvContext, outChan chan<- TOut) error
	Count() int
	CleanUp() error
}

// OperationExecutor is the generic public interface for operation (transform) nodes.
// Implementations consume a value of type TIn and produce a value of type TOut.
type OperationExecutor[TIn, TOut any] interface {
	GetName() string
	GetUniqueIdentifier() string
	Execute(ctx CnvContext, inData TIn) (TOut, error)
	ExecuteLoop(ctx CnvContext, inChan <-chan TIn, outChan chan<- TOut) error
	Count() int
	CleanUp() error
}

// SinkExecutor is the generic public interface for sink nodes. Implementations
// consume values of type TIn without producing output.
type SinkExecutor[TIn any] interface {
	GetName() string
	GetUniqueIdentifier() string
	Execute(ctx CnvContext, inData TIn) error
	ExecuteLoop(ctx CnvContext, inChan <-chan TIn) error
	Count() int
	CleanUp() error
}

// JointExecutor is the generic public interface for joint nodes. Joints
// connect multiple input channels to multiple output channels.
type JointExecutor[TIn, TOut any] interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChans []chan TIn, outChans []chan TOut) error
	Count() int
	InputCount() int
	OutputCount() int
}

// ---------------------------------------------------------------------------
// sourceWrapper
// ---------------------------------------------------------------------------

// sourceWrapper adapts a SourceExecutor[TOut] to the nodeExecutor interface.
// Because sources have no input, InType always returns nil. The channel bridge in
// executeLoopUntyped only needs to forward TOut values onto the untyped outChan.
type sourceWrapper[TOut any] struct {
	exec SourceExecutor[TOut]
}

// wrapSource returns a nodeExecutor that delegates to the given SourceExecutor.
func wrapSource[TOut any](exec SourceExecutor[TOut]) nodeExecutor {
	return &sourceWrapper[TOut]{exec: exec}
}

func (w *sourceWrapper[TOut]) GetName() string {
	return w.exec.GetName()
}

func (w *sourceWrapper[TOut]) GetUniqueIdentifier() string {
	return w.exec.GetUniqueIdentifier()
}

// InType returns nil: sources do not consume input.
func (w *sourceWrapper[TOut]) InType() reflect.Type {
	return nil
}

// OutType returns the reflect.Type of TOut.
func (w *sourceWrapper[TOut]) OutType() reflect.Type {
	return reflect.TypeFor[TOut]()
}

// WorkerType identifies this as a source worker.
func (w *sourceWrapper[TOut]) WorkerType() string {
	return WorkerTypeSource
}

// executeUntyped calls the underlying Execute method. inData is ignored because
// sources produce data autonomously. The returned value is boxed as any.
func (w *sourceWrapper[TOut]) executeUntyped(ctx CnvContext, inData any) (any, error) {
	out, err := w.exec.Execute(ctx)
	if err != nil {
		return nil, err
	}
	return any(out), nil
}

// executeLoopUntyped creates a typed chan TOut, launches a goroutine to forward
// each TOut value onto outChan as an any, then calls the underlying ExecuteLoop.
// The typed channel is closed once ExecuteLoop returns, and the forwarding goroutine
// is joined before this method returns so the caller sees a fully drained pipeline.
func (w *sourceWrapper[TOut]) executeLoopUntyped(ctx CnvContext, inChan <-chan any, outChan chan<- any) error {
	typedOut := make(chan TOut)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range typedOut {
			outChan <- any(v)
		}
	}()

	err := w.exec.ExecuteLoop(ctx, typedOut)
	// Signal the forwarding goroutine that no more values will arrive.
	close(typedOut)
	// Wait until every value has been forwarded before returning.
	wg.Wait()
	return err
}

func (w *sourceWrapper[TOut]) Count() int {
	return w.exec.Count()
}

func (w *sourceWrapper[TOut]) CleanUp() error {
	return w.exec.CleanUp()
}

// ---------------------------------------------------------------------------
// operationWrapper
// ---------------------------------------------------------------------------

// operationWrapper adapts an OperationExecutor[TIn, TOut] to the nodeExecutor
// interface. It bridges the untyped inChan to a typed chan TIn, and bridges a typed
// chan TOut back to the untyped outChan.
type operationWrapper[TIn, TOut any] struct {
	exec OperationExecutor[TIn, TOut]
}

// wrapOperation returns a nodeExecutor that delegates to the given OperationExecutor.
func wrapOperation[TIn, TOut any](exec OperationExecutor[TIn, TOut]) nodeExecutor {
	return &operationWrapper[TIn, TOut]{exec: exec}
}

func (w *operationWrapper[TIn, TOut]) GetName() string {
	return w.exec.GetName()
}

func (w *operationWrapper[TIn, TOut]) GetUniqueIdentifier() string {
	return w.exec.GetUniqueIdentifier()
}

// InType returns the reflect.Type of TIn.
func (w *operationWrapper[TIn, TOut]) InType() reflect.Type {
	return reflect.TypeFor[TIn]()
}

// OutType returns the reflect.Type of TOut.
func (w *operationWrapper[TIn, TOut]) OutType() reflect.Type {
	return reflect.TypeFor[TOut]()
}

// WorkerType identifies this as an operation worker.
func (w *operationWrapper[TIn, TOut]) WorkerType() string {
	return WorkerTypeOperation
}

// executeUntyped casts inData to TIn, calls the underlying Execute method, and
// boxes the result as any.
func (w *operationWrapper[TIn, TOut]) executeUntyped(ctx CnvContext, inData any) (any, error) {
	typedIn := inData.(TIn)
	out, err := w.exec.Execute(ctx, typedIn)
	if err != nil {
		return nil, err
	}
	return any(out), nil
}

// executeLoopUntyped creates typed channels for both directions:
//   - A goroutine bridges inChan (chan any) → typedIn (chan TIn).
//   - A goroutine bridges typedOut (chan TOut) → outChan (chan any).
//
// ExecuteLoop is the owner of typedOut: it writes to it and the caller is responsible
// for closing it once ExecuteLoop returns. typedIn is closed when inChan is closed,
// which propagates the upstream shutdown signal naturally.
func (w *operationWrapper[TIn, TOut]) executeLoopUntyped(ctx CnvContext, inChan <-chan any, outChan chan<- any) error {
	typedIn := make(chan TIn)
	typedOut := make(chan TOut)

	// Bridge any → TIn: close typedIn when inChan is exhausted so that the
	// wrapped executor observes the normal end-of-input signal.
	go func() {
		defer close(typedIn)
		for v := range inChan {
			typedIn <- v.(TIn)
		}
	}()

	// Bridge TOut → any: drain typedOut and forward each value onto outChan.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := range typedOut {
			outChan <- any(v)
		}
	}()

	err := w.exec.ExecuteLoop(ctx, typedIn, typedOut)
	// The executor has finished writing; close typedOut so the forwarding goroutine
	// knows there are no more values to drain.
	close(typedOut)
	// Wait until all output values have been forwarded before returning.
	wg.Wait()
	return err
}

func (w *operationWrapper[TIn, TOut]) Count() int {
	return w.exec.Count()
}

func (w *operationWrapper[TIn, TOut]) CleanUp() error {
	return w.exec.CleanUp()
}

// ---------------------------------------------------------------------------
// sinkWrapper
// ---------------------------------------------------------------------------

// sinkWrapper adapts a SinkExecutor[TIn] to the nodeExecutor interface.
// Because sinks produce no output, OutType always returns nil. The channel bridge
// in executeLoopUntyped only needs to convert inChan (chan any) to a typed chan TIn.
type sinkWrapper[TIn any] struct {
	exec SinkExecutor[TIn]
}

// wrapSink returns a nodeExecutor that delegates to the given SinkExecutor.
func wrapSink[TIn any](exec SinkExecutor[TIn]) nodeExecutor {
	return &sinkWrapper[TIn]{exec: exec}
}

func (w *sinkWrapper[TIn]) GetName() string {
	return w.exec.GetName()
}

func (w *sinkWrapper[TIn]) GetUniqueIdentifier() string {
	return w.exec.GetUniqueIdentifier()
}

// InType returns the reflect.Type of TIn.
func (w *sinkWrapper[TIn]) InType() reflect.Type {
	return reflect.TypeFor[TIn]()
}

// OutType returns nil: sinks do not produce output.
func (w *sinkWrapper[TIn]) OutType() reflect.Type {
	return nil
}

// WorkerType identifies this as a sink worker.
func (w *sinkWrapper[TIn]) WorkerType() string {
	return WorkerTypeSink
}

// executeUntyped casts inData to TIn, calls the underlying Execute method, and
// always returns nil as the output value.
func (w *sinkWrapper[TIn]) executeUntyped(ctx CnvContext, inData any) (any, error) {
	typedIn := inData.(TIn)
	if err := w.exec.Execute(ctx, typedIn); err != nil {
		return nil, err
	}
	return nil, nil
}

// executeLoopUntyped creates a typed chan TIn and launches a goroutine that reads
// from inChan and forwards each value after casting. The typed channel is closed
// when inChan closes, propagating the shutdown signal to the wrapped executor.
// outChan is unused because sinks produce no output.
func (w *sinkWrapper[TIn]) executeLoopUntyped(ctx CnvContext, inChan <-chan any, outChan chan<- any) error {
	typedIn := make(chan TIn)

	// Bridge any → TIn: close typedIn when the upstream source is exhausted.
	go func() {
		defer close(typedIn)
		for v := range inChan {
			typedIn <- v.(TIn)
		}
	}()

	return w.exec.ExecuteLoop(ctx, typedIn)
}

func (w *sinkWrapper[TIn]) Count() int {
	return w.exec.Count()
}

func (w *sinkWrapper[TIn]) CleanUp() error {
	return w.exec.CleanUp()
}

// ---------------------------------------------------------------------------
// jointWrapper
// ---------------------------------------------------------------------------

// jointWrapper adapts a JointExecutor[TIn, TOut] to the jointExecutor
// interface. It bridges slices of untyped chan any to typed channel slices in both
// the input and output directions.
type jointWrapper[TIn, TOut any] struct {
	exec JointExecutor[TIn, TOut]
}

// wrapJoint returns a jointExecutor that delegates to the given JointExecutor.
func wrapJoint[TIn, TOut any](exec JointExecutor[TIn, TOut]) jointExecutor {
	return &jointWrapper[TIn, TOut]{exec: exec}
}

func (w *jointWrapper[TIn, TOut]) GetName() string {
	return w.exec.GetName()
}

func (w *jointWrapper[TIn, TOut]) GetUniqueIdentifier() string {
	return w.exec.GetUniqueIdentifier()
}

// InType returns the reflect.Type of TIn.
func (w *jointWrapper[TIn, TOut]) InType() reflect.Type {
	return reflect.TypeFor[TIn]()
}

// OutType returns the reflect.Type of TOut.
func (w *jointWrapper[TIn, TOut]) OutType() reflect.Type {
	return reflect.TypeFor[TOut]()
}

// executeLoopUntyped bridges all input and output channels between the untyped
// any domain and the typed domain expected by the wrapped executor:
//
//   - For each inChans[i] (chan any), a goroutine reads values, casts them to TIn,
//     and forwards them to typedInChans[i] (chan TIn). The typed channel is closed
//     when the corresponding untyped channel closes.
//
//   - For each outChans[i] (chan any), a goroutine reads from typedOutChans[i]
//     (chan TOut) and forwards values onto outChans[i]. These goroutines are started
//     before ExecuteLoop so they are ready to drain as soon as the executor writes.
//
// After ExecuteLoop returns, all typed output channels are closed so that the
// forwarding goroutines can finish draining. The WaitGroup ensures every value has
// been forwarded before this method returns.
func (w *jointWrapper[TIn, TOut]) executeLoopUntyped(ctx CnvContext, inChans []chan any, outChans []chan any) error {
	typedInChans := make([]chan TIn, len(inChans))
	typedOutChans := make([]chan TOut, len(outChans))

	var wg sync.WaitGroup

	// Set up input bridges: untyped any → typed TIn.
	for i, inCh := range inChans {
		typedInChans[i] = make(chan TIn)
		wg.Add(1)
		go func(src chan any, dst chan TIn) {
			defer wg.Done()
			defer close(dst)
			for v := range src {
				dst <- v.(TIn)
			}
		}(inCh, typedInChans[i])
	}

	// Set up output bridges: typed TOut → untyped any.
	for i := range outChans {
		typedOutChans[i] = make(chan TOut)
		wg.Add(1)
		go func(src chan TOut, dst chan any) {
			defer wg.Done()
			for v := range src {
				dst <- any(v)
			}
		}(typedOutChans[i], outChans[i])
	}

	err := w.exec.ExecuteLoop(ctx, typedInChans, typedOutChans)
	// The executor has finished writing to all typed output channels; close them so
	// the forwarding goroutines know there are no more values to drain.
	for _, ch := range typedOutChans {
		close(ch)
	}
	// Wait for all bridge goroutines (both input and output) to finish.
	wg.Wait()
	return err
}

func (w *jointWrapper[TIn, TOut]) Count() int {
	return w.exec.Count()
}

func (w *jointWrapper[TIn, TOut]) InputCount() int {
	return w.exec.InputCount()
}

func (w *jointWrapper[TIn, TOut]) OutputCount() int {
	return w.exec.OutputCount()
}

// ---------------------------------------------------------------------------
// ConcreteSourceExecutor
// ---------------------------------------------------------------------------

// ConcreteSourceExecutor is a base struct that provides default implementations
// of the SourceExecutor interface. Embed a pointer to this type in your own
// source struct and override Execute or ExecuteLoop (or both) as needed.
//
// Example:
//
//	type MySource struct {
//	    *conveyor.ConcreteSourceExecutor[MyRecord]
//	    db *sql.DB
//	}
//
//	func (s *MySource) Execute(ctx conveyor.CnvContext) (MyRecord, error) {
//	    // fetch and return the next record
//	}
type ConcreteSourceExecutor[TOut any] struct {
	// Name is the human-readable identifier for this executor.
	Name string
	// Data holds any auxiliary data the executor may need at runtime.
	Data interface{}
}

// GetName returns the name of the executor.
func (cse *ConcreteSourceExecutor[TOut]) GetName() string {
	return cse.Name
}

// GetUniqueIdentifier returns a unique string identifying this executor instance.
func (cse *ConcreteSourceExecutor[TOut]) GetUniqueIdentifier() string {
	return cse.Name
}

// Count returns the default number of goroutines (1) for this executor.
// Override in your embedding struct to increase parallelism.
func (cse *ConcreteSourceExecutor[TOut]) Count() int {
	return 1
}

// CleanUp performs any post-execution cleanup. The default implementation is a
// no-op. Override in your embedding struct if resources must be released.
func (cse *ConcreteSourceExecutor[TOut]) CleanUp() error {
	return nil
}

// Execute is the default implementation that returns ErrExecuteNotImplemented.
// You must override this method in your embedding struct when using WorkerModeTransaction.
func (cse *ConcreteSourceExecutor[TOut]) Execute(ctx CnvContext) (TOut, error) {
	var zero TOut
	return zero, ErrExecuteNotImplemented
}

// ExecuteLoop is the default implementation that returns ErrExecuteNotImplemented.
// You must override this method in your embedding struct when using WorkerModeLoop.
func (cse *ConcreteSourceExecutor[TOut]) ExecuteLoop(ctx CnvContext, outChan chan<- TOut) error {
	return ErrExecuteNotImplemented
}

// ---------------------------------------------------------------------------
// ConcreteOperationExecutor
// ---------------------------------------------------------------------------

// ConcreteOperationExecutor is a base struct that provides default implementations
// of the OperationExecutor interface. Embed a pointer to this type in your own
// operation struct and override Execute or ExecuteLoop (or both) as needed.
//
// Example:
//
//	type MyTransformer struct {
//	    *conveyor.ConcreteOperationExecutor[RawRecord, EnrichedRecord]
//	}
//
//	func (t *MyTransformer) Execute(ctx conveyor.CnvContext, in RawRecord) (EnrichedRecord, error) {
//	    // transform and return
//	}
type ConcreteOperationExecutor[TIn, TOut any] struct {
	// Name is the human-readable identifier for this executor.
	Name string
	// Data holds any auxiliary data the executor may need at runtime.
	Data interface{}
}

// GetName returns the name of the executor.
func (coe *ConcreteOperationExecutor[TIn, TOut]) GetName() string {
	return coe.Name
}

// GetUniqueIdentifier returns a unique string identifying this executor instance.
func (coe *ConcreteOperationExecutor[TIn, TOut]) GetUniqueIdentifier() string {
	return coe.Name
}

// Count returns the default number of goroutines (1) for this executor.
// Override in your embedding struct to increase parallelism.
func (coe *ConcreteOperationExecutor[TIn, TOut]) Count() int {
	return 1
}

// CleanUp performs any post-execution cleanup. The default implementation is a
// no-op. Override in your embedding struct if resources must be released.
func (coe *ConcreteOperationExecutor[TIn, TOut]) CleanUp() error {
	return nil
}

// Execute is the default implementation that returns ErrExecuteNotImplemented.
// You must override this method in your embedding struct when using WorkerModeTransaction.
func (coe *ConcreteOperationExecutor[TIn, TOut]) Execute(ctx CnvContext, inData TIn) (TOut, error) {
	var zero TOut
	return zero, ErrExecuteNotImplemented
}

// ExecuteLoop is the default implementation that returns ErrExecuteNotImplemented.
// You must override this method in your embedding struct when using WorkerModeLoop.
func (coe *ConcreteOperationExecutor[TIn, TOut]) ExecuteLoop(ctx CnvContext, inChan <-chan TIn, outChan chan<- TOut) error {
	return ErrExecuteNotImplemented
}

// ---------------------------------------------------------------------------
// ConcreteSinkExecutor
// ---------------------------------------------------------------------------

// ConcreteSinkExecutor is a base struct that provides default implementations
// of the SinkExecutor interface. Embed a pointer to this type in your own
// sink struct and override Execute or ExecuteLoop (or both) as needed.
//
// Example:
//
//	type MyWriter struct {
//	    *conveyor.ConcreteSinkExecutor[EnrichedRecord]
//	    writer *bufio.Writer
//	}
//
//	func (w *MyWriter) Execute(ctx conveyor.CnvContext, in EnrichedRecord) error {
//	    // write the record
//	}
type ConcreteSinkExecutor[TIn any] struct {
	// Name is the human-readable identifier for this executor.
	Name string
	// Data holds any auxiliary data the executor may need at runtime.
	Data interface{}
}

// GetName returns the name of the executor.
func (cse *ConcreteSinkExecutor[TIn]) GetName() string {
	return cse.Name
}

// GetUniqueIdentifier returns a unique string identifying this executor instance.
func (cse *ConcreteSinkExecutor[TIn]) GetUniqueIdentifier() string {
	return cse.Name
}

// Count returns the default number of goroutines (1) for this executor.
// Override in your embedding struct to increase parallelism.
func (cse *ConcreteSinkExecutor[TIn]) Count() int {
	return 1
}

// CleanUp performs any post-execution cleanup. The default implementation is a
// no-op. Override in your embedding struct if resources must be released.
func (cse *ConcreteSinkExecutor[TIn]) CleanUp() error {
	return nil
}

// Execute is the default implementation that returns ErrExecuteNotImplemented.
// You must override this method in your embedding struct when using WorkerModeTransaction.
func (cse *ConcreteSinkExecutor[TIn]) Execute(ctx CnvContext, inData TIn) error {
	return ErrExecuteNotImplemented
}

// ExecuteLoop is the default implementation that returns ErrExecuteNotImplemented.
// You must override this method in your embedding struct when using WorkerModeLoop.
func (cse *ConcreteSinkExecutor[TIn]) ExecuteLoop(ctx CnvContext, inChan <-chan TIn) error {
	return ErrExecuteNotImplemented
}

// ---------------------------------------------------------------------------
// ConcreteJointExecutor
// ---------------------------------------------------------------------------

// ConcreteJointExecutor is a base struct that provides default
// implementations of the JointExecutor interface. Embed a pointer to
// this type in your own joint struct and override ExecuteLoop as needed.
//
// Example:
//
//	type MyBroadcast struct {
//	    *conveyor.ConcreteJointExecutor[Record, Record]
//	}
//
//	func (b *MyBroadcast) ExecuteLoop(ctx conveyor.CnvContext, inChans []chan Record, outChans []chan Record) error {
//	    // fan-out each item from inChans[0] to all outChans
//	}
type ConcreteJointExecutor[TIn, TOut any] struct {
	// Name is the human-readable identifier for this executor.
	Name string
}

// GetName returns the name of the executor.
func (cje *ConcreteJointExecutor[TIn, TOut]) GetName() string {
	return cje.Name
}

// GetUniqueIdentifier returns a unique string identifying this executor instance.
func (cje *ConcreteJointExecutor[TIn, TOut]) GetUniqueIdentifier() string {
	return cje.Name
}

// Count returns the default number of goroutines (1) for this executor.
// Override in your embedding struct to increase parallelism.
func (cje *ConcreteJointExecutor[TIn, TOut]) Count() int {
	return 1
}

// InputCount returns the default number of input channels (1) for this joint.
// Override in your embedding struct for fan-in scenarios.
func (cje *ConcreteJointExecutor[TIn, TOut]) InputCount() int {
	return 1
}

// OutputCount returns the default number of output channels (1) for this joint.
// Override in your embedding struct for fan-out scenarios.
func (cje *ConcreteJointExecutor[TIn, TOut]) OutputCount() int {
	return 1
}

// ExecuteLoop is the default implementation that returns ErrExecuteNotImplemented.
// You must override this method in your embedding struct.
func (cje *ConcreteJointExecutor[TIn, TOut]) ExecuteLoop(ctx CnvContext, inChans []chan TIn, outChans []chan TOut) error {
	return ErrExecuteNotImplemented
}
