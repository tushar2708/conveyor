# Migrating Conveyor to Go Generics

This document explains how the `conveyor` library was migrated from an untyped
`map[string]interface{}` API to a fully generic, type-safe API using Go 1.18+
type parameters. It covers what changed for users first, then walks through the
internal architecture that makes it all work.

---

## Part 1: What Changed for Users

### Before: Everything being passed from one node to the other was `map[string]interface{}`

The library exposed a single `NodeExecutor` interface. Every node in
the pipeline - source, operation, or sink - implemented the same interface, and
all data flowed as `map[string]interface{}`:

```go
// OLD API
type NodeExecutor interface {
    GetName() string
    GetUniqueIdentifier() string
    Execute(ctx CnvContext, inData map[string]interface{}) (map[string]interface{}, error)
    ExecuteLoop(ctx CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error
    Count() int
    CleanUp() error
}
```

A typical node implementation looked like this:

```go
// OLD: SquareOperator
type SquareOperator struct {
    *conveyor.ConcreteNodeExecutor
}

func (o *SquareOperator) Execute(ctx conveyor.CnvContext, in map[string]interface{}) (map[string]interface{}, error) {
    num := in["number"].(int)           // manual type assertion, panics if wrong
    return map[string]interface{}{
        "number": num * num,
    }, nil
}
```

And pipeline construction used method calls with explicit worker-type strings:

```go
// OLD: Adding nodes
cnv.AddNodeExecutor(src, conveyor.WorkerModeLoop, "SOURCE_WORKER")
cnv.AddNodeExecutor(op, conveyor.WorkerModeTransaction, "OPERATION_WORKER")
cnv.AddNodeExecutor(snk, conveyor.WorkerModeTransaction, "SINK_WORKER")
```

**Problems:**

1. No compile-time type checking. You could wire a node that produces `string`
   into one that expects `int` and the compiler wouldn't complain. You'd get a
   runtime panic inside `Execute` when the type assertion failed.
2. Data keys were arbitrary strings (`in["number"]`). Typo a key name and you'd
   get a nil value at runtime.
3. Every node used the same `NodeExecutor` interface even though sources don't
   consume input, sinks don't produce output, and operations do both. The API
   couldn't enforce these constraints.
4. Worker type had to be passed as a magic string (`"SOURCE_WORKER"`).

### After: Typed Generics with Compile-Time Validation

The new API splits the single `NodeExecutor` into four distinct generic
interfaces - one for each role in the pipeline:

```go
// NEW API - each role has its own interface with appropriate type parameters

type SourceExecutor[TOut any] interface {
    GetName() string
    GetUniqueIdentifier() string
    Execute(ctx CnvContext) (TOut, error)               // produces TOut, no input
    ExecuteLoop(ctx CnvContext, outChan chan<- TOut) error
    Count() int
    CleanUp() error
}

type OperationExecutor[TIn, TOut any] interface {
    GetName() string
    GetUniqueIdentifier() string
    Execute(ctx CnvContext, inData TIn) (TOut, error)    // TIn → TOut
    ExecuteLoop(ctx CnvContext, inChan <-chan TIn, outChan chan<- TOut) error
    Count() int
    CleanUp() error
}

type SinkExecutor[TIn any] interface {
    GetName() string
    GetUniqueIdentifier() string
    Execute(ctx CnvContext, inData TIn) error             // consumes TIn, no output
    ExecuteLoop(ctx CnvContext, inChan <-chan TIn) error
    Count() int
    CleanUp() error
}

type JointExecutor[TIn, TOut any] interface {
    GetName() string
    GetUniqueIdentifier() string
    ExecuteLoop(ctx CnvContext, inChans []chan TIn, outChans []chan TOut) error
    Count() int
    InputCount() int
    OutputCount() int
}
```

A node implementation now works directly with typed values:

```go
// NEW: SquareOperator - no maps, no type assertions
type SquareOperator struct {
    *conveyor.ConcreteOperationExecutor[int, int]
}

func (o *SquareOperator) Execute(ctx conveyor.CnvContext, num int) (int, error) {
    return num * num, nil
}
```

Pipeline construction uses standalone generic functions that infer the worker type
from the function name and validate adjacent types at construction time:

```go
// NEW: Pipeline construction with compile-time type safety
conveyor.AddSource[int](cnv, src, conveyor.WorkerModeLoop)
conveyor.AddOperation[int, int](cnv, op, conveyor.WorkerModeTransaction)
conveyor.AddSink[int](cnv, snk, conveyor.WorkerModeTransaction)
```

If you try to wire incompatible types, you get an error immediately:

```go
// Source outputs int, but operation expects string - caught at construction time
conveyor.AddSource[int](cnv, src, conveyor.WorkerModeLoop)
err := conveyor.AddOperation[string, string](cnv, op, conveyor.WorkerModeTransaction)
// err wraps ErrTypeMismatch: "expected input type int but got string"
```

For cases where a type mismatch is unambiguously a programmer bug, `Must*`
variants panic instead of returning an error:

```go
// Panics immediately if types don't match - ideal for init-time setup
conveyor.MustAddSource[int](cnv, src, conveyor.WorkerModeLoop)
conveyor.MustAddOperation[int, int](cnv, op, conveyor.WorkerModeTransaction)
conveyor.MustAddSink[int](cnv, snk, conveyor.WorkerModeTransaction)
```

### Side-by-Side: Full Pipeline Example

**Before:**

```go
type NumberSource struct {
    *conveyor.ConcreteNodeExecutor
    CountLimit int
}

func (src *NumberSource) ExecuteLoop(ctx conveyor.CnvContext,
    inChan <-chan map[string]interface{},
    outChan chan<- map[string]interface{}) error {

    for i := 0; i <= src.CountLimit; i++ {
        outChan <- map[string]interface{}{"number": i}
    }
    return nil
}

// ... SquareOperator, PrinterSink similarly use map[string]interface{}

cnv.AddNodeExecutor(gen, conveyor.WorkerModeLoop, "SOURCE_WORKER")
cnv.AddNodeExecutor(sqr, conveyor.WorkerModeTransaction, "OPERATION_WORKER")
cnv.AddNodeExecutor(prnt, conveyor.WorkerModeTransaction, "SINK_WORKER")
```

**After (generics):**

```go
type NumberSource struct {
    *conveyor.ConcreteSourceExecutor[int]
    CountLimit int
}

func (src *NumberSource) ExecuteLoop(ctx conveyor.CnvContext, outChan chan<- int) error {
    for i := 0; i <= src.CountLimit; i++ {
        outChan <- i
    }
    return nil
}

type SquareOperator struct {
    *conveyor.ConcreteOperationExecutor[int, int]
}

func (o *SquareOperator) ExecuteLoop(ctx conveyor.CnvContext,
    inChan <-chan int, outChan chan<- int) error {
    for num := range inChan {
        outChan <- num * num
    }
    return nil
}

type PrinterSink struct {
    *conveyor.ConcreteSinkExecutor[int]
}

func (snk *PrinterSink) ExecuteLoop(ctx conveyor.CnvContext, inChan <-chan int) error {
    for num := range inChan {
        fmt.Printf("Output: %d\n", num)
    }
    return nil
}

conveyor.AddSource[int](cnv, gen, conveyor.WorkerModeLoop)
conveyor.AddOperation[int, int](cnv, sqr, conveyor.WorkerModeTransaction)
conveyor.AddSink[int](cnv, prnt, conveyor.WorkerModeTransaction)
```

Notice that the source's `ExecuteLoop` no longer receives an input channel
(sources don't consume input), and the sink's `ExecuteLoop` no longer receives an
output channel (sinks don't produce output). The API enforces what was previously
just a convention.

---

## Part 2: How Construction-Time Type Validation Works

When you call `AddSource[int](cnv, ...)`, the conveyor records the type `int` in
a field called `lastNodeOutType` using `reflect.TypeFor[int]()`. When the next
call is `AddOperation[int, string](cnv, ...)`, the function computes
`reflect.TypeFor[int]()` and checks it against `lastNodeOutType`. If they don't
match, it returns `ErrTypeMismatch` immediately - before any workers are started.

Here's the actual code in `conveyor.go`:

```go
func AddSource[TOut any](cnv *Conveyor, exec SourceExecutor[TOut], mode WorkerMode) error {
    // ... create worker, add to conveyor ...
    cnv.lastNodeOutType = reflect.TypeFor[TOut]()   // record: this source outputs TOut
    return nil
}

func AddOperation[TIn, TOut any](cnv *Conveyor, exec OperationExecutor[TIn, TOut], mode WorkerMode) error {
    expectedIn := reflect.TypeFor[TIn]()
    if cnv.lastNodeOutType != nil && cnv.lastNodeOutType != expectedIn {
        return fmt.Errorf("%w: expected input type %v but got %v",
            ErrTypeMismatch, cnv.lastNodeOutType, expectedIn)
    }
    // ... create worker, add to conveyor ...
    cnv.lastNodeOutType = reflect.TypeFor[TOut]()   // update: this operation outputs TOut
    return nil
}

func AddSink[TIn any](cnv *Conveyor, exec SinkExecutor[TIn], mode WorkerMode) error {
    expectedIn := reflect.TypeFor[TIn]()
    if cnv.lastNodeOutType != nil && cnv.lastNodeOutType != expectedIn {
        return fmt.Errorf("%w: expected input type %v but got %v",
            ErrTypeMismatch, cnv.lastNodeOutType, expectedIn)
    }
    // ... create worker, add to conveyor ...
    cnv.lastNodeOutType = nil   // sinks produce no output - clear the tracker
    return nil
}
```

For joints (fan-out/fan-in nodes), a second tracker `lastJointOutType` is used:

```go
func AddJointAfterNode[TIn, TOut any](cnv *Conveyor, exec JointExecutor[TIn, TOut]) error {
    expectedIn := reflect.TypeFor[TIn]()
    if cnv.lastNodeOutType != nil && cnv.lastNodeOutType != expectedIn {
        return fmt.Errorf("%w: ...", ErrTypeMismatch, ...)
    }
    // ... wrap, add to conveyor, link to previous node ...
    cnv.lastNodeOutType = nil                          // can't chain linear nodes after a joint
    cnv.lastJointOutType = reflect.TypeFor[TOut]()     // downstream must use AddSinkAfterJoint
    return nil
}

func AddSinkAfterJoint[TIn any](cnv *Conveyor, exec SinkExecutor[TIn], mode WorkerMode) error {
    expectedIn := reflect.TypeFor[TIn]()
    if cnv.lastJointOutType != nil && cnv.lastJointOutType != expectedIn {
        return fmt.Errorf("%w: ...", ErrTypeMismatch, ...)
    }
    // ... wire to joint's next output channel ...
    return nil
}
```

The type tracker state machine looks like this:

```
AddSource[int]            → lastNodeOutType=int,    lastJointOutType=nil
AddOperation[int,string]  → lastNodeOutType=string, lastJointOutType=nil
AddJointAfterNode[string] → lastNodeOutType=nil,    lastJointOutType=string
AddSinkAfterJoint[string] → lastNodeOutType=nil,    lastJointOutType=string (unchanged)
```

This means **types are validated at pipeline construction time**, not at runtime
when data is flowing. If you get past all your `Add*` calls without an error,
every adjacent pair of nodes is guaranteed to have compatible types.

---

## Part 3: How the Internal Type Erasure Works

This is the most important architectural decision. Internally, the worker pool
infrastructure - `SourceWorkerPool`, `OperationWorkerPool`, `SinkWorkerPool`,
`JointWorkerPool` - is completely untyped. All channels between workers are
`chan any`. The generics only exist at the public API boundary.

Here's why: the worker pools manage goroutine lifecycles, semaphores, shutdown
signals, panic recovery, and channel linking. None of that logic cares about the
type of data flowing through the channels. Parameterizing all of that
infrastructure with generics would have added complexity for zero benefit -
especially since Go doesn't support generic methods on non-generic types, which
would have required restructuring the entire `NodeWorker` / `JointWorker`
interface hierarchy.

So the architecture has two layers, connected by **wrapper adapters**.

### Layer 1: The Public Generic API

Users implement typed interfaces like `SourceExecutor[int]` or
`OperationExecutor[int, string]`. Their `Execute` and `ExecuteLoop` methods work
with concrete types - `int`, `string`, custom structs - no type assertions needed.

### Layer 2: The Internal Untyped Infrastructure

Worker pools operate on an internal `nodeExecutor` interface (unexported) that
speaks `chan any`:

```go
type nodeExecutor interface {
    GetName() string
    GetUniqueIdentifier() string
    InType() reflect.Type
    OutType() reflect.Type
    WorkerType() string
    executeUntyped(ctx CnvContext, inData any) (any, error)
    executeLoopUntyped(ctx CnvContext, inChan <-chan any, outChan chan<- any) error
    Count() int
    CleanUp() error
}
```

The worker pool code calls `executeUntyped` or `executeLoopUntyped` and passes
`any` values through `chan any`. It doesn't know or care about the concrete type.
For example, here's how `OperationWorkerPool` processes a single item in
transaction mode (`operationworker.go`):

```go
go func(data any) {
    defer fwp.sem.Release(1)
    out, err := fwp.Executor.executeUntyped(ctx, data)   // any in, any out
    if err == nil {
        fwp.outputChannel <- out                           // chan any
    }
}(inData)
```

### The Bridge: Wrapper Adapters

The wrappers are where the two layers connect. When you call
`AddSource[int](cnv, mySource, ...)`, the first thing it does is:

```go
wrapped := wrapSource[int](mySource)
```

This returns a `sourceWrapper[int]` that implements `nodeExecutor`. The wrapper
holds a reference to your typed `SourceExecutor[int]` and translates between
typed and untyped worlds.

**How `executeUntyped` works (transaction mode bridge):**

For operations, the wrapper receives an `any` value from the worker pool, type-
asserts it to the expected type, calls the user's typed `Execute` method, and
boxes the result back to `any`:

```go
// operationWrapper.executeUntyped - bridges any → TIn → Execute → TOut → any
func (w *operationWrapper[TIn, TOut]) executeUntyped(ctx CnvContext, inData any) (any, error) {
    typedIn := inData.(TIn)                       // any → TIn (safe: type was validated at construction)
    out, err := w.exec.Execute(ctx, typedIn)      // call user's typed Execute
    if err != nil {
        return nil, err
    }
    return any(out), nil                           // TOut → any
}
```

That `inData.(TIn)` cast is safe because the `Add*` functions already validated
that the upstream node produces `TIn`. The type assertion will never fail at
runtime because the mismatch would have been caught at construction time.

**How `executeLoopUntyped` works (loop mode bridge):**

This is more involved. The worker pool passes `chan any` channels, but the user's
`ExecuteLoop` expects typed channels. The wrapper creates typed intermediate
channels and runs goroutines to shuttle values between the untyped and typed
worlds.

Here's what happens inside `operationWrapper.executeLoopUntyped`, step by step:

```go
func (w *operationWrapper[TIn, TOut]) executeLoopUntyped(
    ctx CnvContext, inChan <-chan any, outChan chan<- any) error {

    typedIn := make(chan TIn)       // ① Create typed intermediate channels
    typedOut := make(chan TOut)

    // ② Input bridge goroutine: reads any from worker pool, casts to TIn,
    //    forwards to the typed channel the user's code reads from.
    //    Closes typedIn when inChan closes (propagating upstream shutdown).
    go func() {
        defer close(typedIn)
        for v := range inChan {
            typedIn <- v.(TIn)      // any → TIn
        }
    }()

    // ③ Output bridge goroutine: reads TOut from user's output,
    //    boxes to any, forwards to the worker pool's untyped channel.
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        for v := range typedOut {
            outChan <- any(v)       // TOut → any
        }
    }()

    // ④ Call the user's typed ExecuteLoop - it reads from typedIn and writes to typedOut.
    //    From the user's perspective, these are plain typed channels.
    err := w.exec.ExecuteLoop(ctx, typedIn, typedOut)

    // ⑤ User's ExecuteLoop returned - close typedOut so the output bridge goroutine
    //    knows there are no more values to forward.
    close(typedOut)

    // ⑥ Wait until every value has been forwarded to the untyped channel.
    wg.Wait()
    return err
}
```

Visually, the data flow through an operation node looks like this:

```
Worker Pool (untyped)          Wrapper (bridge)              User Code (typed)
═══════════════════          ══════════════════           ═══════════════════

                             ┌──────────────┐
  inChan (chan any) ────────▶│ input bridge │──▶ typedIn (chan TIn)
       ║                     │  v.(TIn)     │               ║
       ║                     └──────────────┘               ║
       ║                                                    ▼
       ║                                            ExecuteLoop(ctx, typedIn, typedOut)
       ║                                                    ║
       ║                                                    ║
       ║                     ┌───────────────┐              ▼
  outChan (chan any) ◀───────│ output bridge │◀── typedOut (chan TOut)
                             │  any(v)       │
                             └───────────────┘
```

For **sources**, there is no input bridge (sources don't consume input), only an
output bridge. For **sinks**, there is no output bridge (sinks don't produce
output), only an input bridge.

For **joints**, the same pattern applies but with slices of channels - one input
bridge goroutine per input channel and one output bridge goroutine per output
channel:

```go
func (w *jointWrapper[TIn, TOut]) executeLoopUntyped(
    ctx CnvContext, inChans []chan any, outChans []chan any) error {

    typedInChans := make([]chan TIn, len(inChans))
    typedOutChans := make([]chan TOut, len(outChans))

    var wg sync.WaitGroup

    // One input bridge goroutine per input channel
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

    // One output bridge goroutine per output channel
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
    for _, ch := range typedOutChans {
        close(ch)
    }
    wg.Wait()
    return err
}
```

### Why the `inData.(TIn)` Cast Never Fails

This is the key insight that holds the whole design together. There are `any`
values flowing through the system, and there are `.(TIn)` type assertions inside
the wrappers. If those assertions ever failed, you'd get a runtime panic. So why
are they safe?

Because of the construction-time validation. When you build a pipeline:

```go
AddSource[int](cnv, src, ...)        // lastNodeOutType = reflect.TypeFor[int]()
AddOperation[int, string](cnv, ...)  // checks: reflect.TypeFor[int]() == lastNodeOutType ✓
```

The `AddOperation` call verified that its `TIn` (`int`) matches what the source
produces. The source wrapper boxes `int` values as `any`. The operation wrapper
casts them back with `inData.(int)`. Since both sides agreed on `int` at
construction time, the cast is guaranteed to succeed at runtime.

If someone had tried `AddOperation[string, string](cnv, ...)` after an `int`
source, `AddOperation` would have returned `ErrTypeMismatch` and the pipeline
would never start. The unsafe cast inside the wrapper would never execute.

**In summary:** generics enforce types at the public boundary, `reflect.TypeFor`
validates types at construction time, and the internal `any` channels are safe
because they only carry values whose types were already validated.

---

## Part 4: File Layout

| File                 | Contents                                                                                                                                                                                                              |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `executor.go`        | Internal interfaces (`nodeExecutor`, `jointExecutor`), public generic interfaces (`SourceExecutor[T]`, etc.), wrapper adapters (`sourceWrapper`, etc.), and concrete base structs (`ConcreteSourceExecutor[T]`, etc.) |
| `conveyor.go`        | `Conveyor` struct with `lastNodeOutType`/`lastJointOutType` trackers, all `Add*` and `Must*` construction functions, worker lifecycle (`Start`, `Stop`)                                                               |
| `sourceworker.go`    | `SourceWorkerPool` - manages goroutines for sources, calls `executeUntyped` / `executeLoopUntyped` via `chan any`                                                                                                     |
| `operationworker.go` | `OperationWorkerPool` - same pattern for operations, reads from `chan any`, writes to `chan any`                                                                                                                      |
| `sinkworker.go`      | `SinkWorkerPool` - same pattern for sinks, reads from `chan any`, no output                                                                                                                                           |
| `jointworker.go`     | `JointWorkerPool` - manages goroutines for joints, calls `executeLoopUntyped` with `[]chan any` slices                                                                                                                |
| `workerpool.go`      | `ConcreteNodeWorker` base struct, `WorkerMode` constants, shared loop-mode launcher                                                                                                                                   |
| `worker_factory.go`  | Maps worker type strings to pool constructors                                                                                                                                                                         |
| `replicate_joint.go` | `ReplicateJoint[T]` - a built-in `JointExecutor[T, T]` that broadcasts one input to N outputs                                                                                                                         |

---

## Part 5: Summary

The migration achieves compile-time type safety without rewriting the entire
worker pool infrastructure. The design has three distinct safety checkpoints:

1. **Go compiler (compile time):** The generic interfaces enforce that your
   struct's `Execute` method signature matches the type parameters. You can't
   implement `SourceExecutor[int]` with an `Execute` that returns `string`.

2. **Construction-time validation (runtime, before pipeline starts):** The `Add*`
   functions use `reflect.TypeFor[T]()` to verify that each node's input type
   matches the previous node's output type. Mismatches are caught before any
   goroutine is launched.

3. **Wrapper type assertions (runtime, during pipeline execution):** The
   `.(TIn)` casts inside wrappers are the final checkpoint. They are guaranteed
   safe by checkpoint 2, but they serve as a defense-in-depth measure - if
   something were ever wrong, you'd get a clear panic rather than silent data
   corruption.

The internal `chan any` layer was deliberately kept untyped. It handles goroutine
management, semaphores, shutdown propagation, and panic recovery - none of which
depends on the data type. The wrapper adapters absorb all the complexity of
converting between typed and untyped worlds, so neither the user code nor the
worker pool code needs to think about the other's type system.