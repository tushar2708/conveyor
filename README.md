# conveyor

[![GitHub license](https://img.shields.io/github/license/mashape/apistatus.svg)]()
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Ftushar2708%2Fconveyor.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Ftushar2708%2Fconveyor?ref=badge_shield)

[![HitCount](http://hits.dwyl.io/tushar2708/conveyor.svg)](http://hits.dwyl.io/tushar2708/conveyor)

[![Maintainability](https://api.codeclimate.com/v1/badges/e6c8164f8cbf98490fe8/maintainability)](https://codeclimate.com/github/tushar2708/conveyor/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/e6c8164f8cbf98490fe8/test_coverage)](https://codeclimate.com/github/tushar2708/conveyor/test_coverage)

A type-safe Go pipeline library using generics, supporting concurrent pipelines with compile-time type validation between nodes and joints.

> **Breaking Change (v1.1.0):** The API now uses Go generics for compile-time type safety. The old `map[string]interface{}`-based `NodeExecutor` and `JointExecutor` interfaces have been replaced by typed generic equivalents (`SourceExecutor[T]`, `OperationExecutor[TIn, TOut]`, `SinkExecutor[T]`, `JointExecutor[TIn, TOut]`). See [GENERIC_MIGRATION.md](GENERIC_MIGRATION.md) for a detailed migration guide.

**TL;DR:**
A pipeline is a standard concurrency pattern in Go, at least in terms of use case & functionality. And there are multiple ways people create them based on their specific requirements. This project is my attempt to create a generic one that can be used in most, if not all use-cases. If you are already aware of what a pipeline is, you can move on to [examples.](https://github.com/tushar2708/conveyor/tree/master/examples "Conveyor Examples") or [Implementation](https://github.com/tushar2708/conveyor#how-to-implement-your-own-nodes) section.

You can also read more about pipelines from [here](https://blog.golang.org/pipelines "Go Pipelines")

## What does Conveyor do?
*Conveyor* does what the name suggests. Just like a conveyor belt, or a production-line, 
where there is a series of workers, working to prepare a final product.
But each one of them has a specific responsibility, may be, to add a component, 
or to put a sticker on the product, or to wrap it in a shiny packaging.
They all work on one item at a time, but once they are done with their part, 
they can work on the next item in the queue, while the next worker does the next thing, 
at the same time.
Now depending on the time each operation takes, you might want to have more than one worker at any/all stages.

This is exactly what this library does. At it's core, it has **node** workers and **joint** workers. 
What we just discussed are *nodes*. While *joints*, as their name suggests are like the plumbing joints of a pipeline. 
They can distribute, or replicate incoming data across branches.

There are 3 kind of Nodes:

1. **Source Nodes**: These are the nodes that are the source of data. It may be a database, a file, a data stream, 
an API server, or anything that can generate new bits of data. In short, create some data, and send it forward.
2. **Operation Nodes**: These are the nodes that "do something" with the data. They might process/enrich/change/replace 
the data, based on your business logic. The example shows a simple example of squaring/adding operations. 
In your application, it may be making API calls to other services, doing matrix multiplications, etc. 
In short, take data from previous node, and send modified/new data to next node. 
3. **Sink Nodes**: These are supposed to be the last node(s) in the conveyor. This is where you finalise your work, may be send the final data to some other external API/stream, save it to a database/file, or just print it on console.

If we talk about Joints, there can be multiple implementation, based on your need.
For now, there is a built-in joint(*ReplicateJoint*) that conveyor has, 
which replicates same data to be sent to multiple nodes at next stage. More will be added in future.

## How to implement your own nodes and joints?

There are 4 generic interfaces, one for each role in the pipeline:

```go
// SourceExecutor produces values of type TOut
type SourceExecutor[TOut any] interface {
	GetName() string
	GetUniqueIdentifier() string
	Execute(ctx CnvContext) (TOut, error)
	ExecuteLoop(ctx CnvContext, outChan chan<- TOut) error
	Count() int
	CleanUp() error
}

// OperationExecutor transforms TIn into TOut
type OperationExecutor[TIn, TOut any] interface {
	GetName() string
	GetUniqueIdentifier() string
	Execute(ctx CnvContext, inData TIn) (TOut, error)
	ExecuteLoop(ctx CnvContext, inChan <-chan TIn, outChan chan<- TOut) error
	Count() int
	CleanUp() error
}

// SinkExecutor consumes values of type TIn
type SinkExecutor[TIn any] interface {
	GetName() string
	GetUniqueIdentifier() string
	Execute(ctx CnvContext, inData TIn) error
	ExecuteLoop(ctx CnvContext, inChan <-chan TIn) error
	Count() int
	CleanUp() error
}

// JointExecutor connects multiple input channels to multiple output channels
type JointExecutor[TIn, TOut any] interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChans []chan TIn, outChans []chan TOut) error
	Count() int
	InputCount() int
	OutputCount() int
}
```

Types are validated at construction time: if you add an `OperationExecutor[string, int]` after a `SourceExecutor[int]`, you get a compile-time type mismatch. If the type parameters are correct but the pipeline structure is wrong (e.g., adding a sink when no nodes exist), you get a clear runtime error at construction time, before the pipeline starts.

* `GetName()` returns a string to represent the name of the executor.

* `GetUniqueIdentifier()` returns a unique-identifier to recognise the executor (you may just return the name).

* `Count()` decides the concurrency you want to have for your node.

* `CleanUp()` is called once node's job is completed. To do any cleanup activity
(like closing a file/database connection, or to wait till your other go-routines finish)


If you don't want to implement all of these methods, there are concrete base structs you can embed, one for each role:

* `ConcreteSourceExecutor[TOut]` - base struct for source nodes
* `ConcreteOperationExecutor[TIn, TOut]` - base struct for operation nodes
* `ConcreteSinkExecutor[TIn]` - base struct for sink nodes
* `ConcreteJointExecutor[TIn, TOut]` - base struct for joint nodes

Each provides default implementations of `GetName`, `GetUniqueIdentifier`, `Count` (returns 1), and `CleanUp`.
For an actual application where you need concurrency, override `Count()` to return something greater than 1.

You must implement one of the below 2 methods, based on what you want your node to do.
Their default implementation just returns `ErrExecuteNotImplemented` error.

* **Execute()** is the single-item processing method. For a source, it takes no input and returns `TOut`. For an operation, it takes `TIn` and returns `TOut`. For a sink, it takes `TIn` and returns only an error.
This method gets called if you have added your node to work in "Transaction Mode" (check example for more on that). In most cases, you can just get the most out of this method. Here we create a new Go-routine for each request, and number of go-routines running at a given time, is decided by `Count()`. If your `Execute()` for a source node returns an error `ErrSourceExhausted`, conveyor will assume that it's time to finish up, and will stop reading any more data, and will gracefully shutdown after processing already read data. Generally, if any node returns a non-nil error, that particular unit of data would be dropped, and won't be sent to the next node.

* **ExecuteLoop()** gets called if your Executor has been added to work in "Loop Mode".
For a source, it receives only `outChan chan<- TOut`. For an operation, it receives `inChan <-chan TIn` and `outChan chan<- TOut`. For a sink, it receives only `inChan <-chan TIn`.
You will need to run a for loop, to read inputs from `inChan` (if applicable), and write to `outChan` (if applicable).
It is supposed to be a blocking function, but the loop should exit once you are done.
If there are 10 instances of this function running, and all of them return,
Conveyor will take that as a signal to shutdown the node, and the ones that come next to it.
As a rule of thumb, always let your source dictate when to finish up, all others will automatically follow its lead.

### Building a Pipeline

Use the top-level generic functions to add nodes to a conveyor. Types are checked at construction time:

```go
cnv, _ := conveyor.NewConveyor("my-pipeline", 10)

// Source produces int values
conveyor.AddSource[int](cnv, mySource, conveyor.WorkerModeLoop)

// Operation takes int, produces int - type must match the source's output
conveyor.AddOperation[int, int](cnv, mySquarer, conveyor.WorkerModeTransaction)

// Sink consumes int - type must match the operation's output
conveyor.AddSink[int](cnv, myPrinter, conveyor.WorkerModeTransaction)

cnv.Start()
```

Each `Add*` function returns an error if the types don't match or the pipeline structure is invalid.
For cases where a type mismatch is a programmer error, use the `Must*` variants that panic instead:

```go
conveyor.MustAddSource[int](cnv, mySource, conveyor.WorkerModeLoop)
conveyor.MustAddOperation[int, int](cnv, mySquarer, conveyor.WorkerModeTransaction)
conveyor.MustAddSink[int](cnv, myPrinter, conveyor.WorkerModeTransaction)
```

For pipelines with branching (joints), use `AddJointAfterNode`, `AddSinkAfterJoint`, and `AddOperationAfterJoint`:

```go
joint, _ := conveyor.NewReplicateJoint[int]("replicator", 3)
conveyor.AddJointAfterNode[int, int](cnv, joint)

conveyor.AddSinkAfterJoint[int](cnv, sink1, conveyor.WorkerModeTransaction)
conveyor.AddSinkAfterJoint[int](cnv, sink2, conveyor.WorkerModeTransaction)
conveyor.AddSinkAfterJoint[int](cnv, sink3, conveyor.WorkerModeTransaction)
```

### Why did I go for the approach of "Implementing an interface", and not "Writing a function" for each node, like the one mentioned [here](https://blog.golang.org/pipelines) ?
A function is what I had started with, but soon realised that if I am going to do anything flexible, I need something
more than a single function. For example, I might want to run a SQL query while creating a node, and fetch the results inside the source node, before starting the conveyor machinery (say, inside a `func New MySQLSource()`),
and then with each call of `Execute()`, I would just return the next entry.
Or if I am using `ExecuteLoop()` I can just keep writing to the output channel inside a for loop.

I couldn't do it elegantly using just a function. Also the `Count()` lets each node decide,
 how many concurrent go-routines should run for it. Which brings us to the next important gotcha- Always keep your `Execute()` & `ExecuteLoop()` methods **race-free**.
 Don't modify any of the receiver struct's parameters, inside these 2 functions.
 Just work with the incoming data, and stream the output to the next channel(s)
 If your read operation(for source), isn't race-free (eg. reading from a file),
 consider using a single go-routine for source, and more for heavy lifting in later nodes.

## Monitoring, Logging, Progress tracking, and Timeout/Killing.

This is what I believe, is a good to have for anything that solves real world problems. 
Continue with this section, when you are really building something with `Conveyor`, 
and have run/gone through basic examples.

If you have noticed, the first argument in both `Execute()` & `ExecuteLoop()` is an interface `ctx CnvContext`.
There is already a default implementation, that you get when you create a new conveyor instance, using:
* `NewConveyor()`

You can also implement your own, and pass it while creating a conveyor using:
* `NewConveyor().SetCustomContext(ctx)`

This is all that you can do with this Context, with default implementation:

* **Logging**: Conveyor doesn't write it's internal logs, rather it keeps publishing the logs to a channel that you can read from.
by calling `conveyorInstance.Logs()`, you can just keep a go-routine running, to print/log those messages. 
This way, you can use your own logging library, in place of getting stuck with the one of my choice.

* **Monitoring**: If your implementation wants to convey some status messages like `Running MySQL Query`, 
`Processed 5 batches of 100 requests`, etc, just call `ctx.SendStatus("Started doing something cool")` 
to publish these status messages. You can read them just like logs, 
by reading from channel given by `conveyorInstance.Status()`.

* **Killing**: If you want to kill your conveyor, before your source has done reading the input,
just call either `conveyorInstance.Stop()` or `ctx.Cancel()` (whichever is handy). 
It will signal all the internal go-routines to shutdown. 
However, if you have started your own go-routines running long loops, or if you are using `ExecuteLoop()`, 
you will have to keep monitoring `ctx.Done()` frequently, 
to make sure that you don't leak out your go-routines, when you kill a conveyor.
This small stub will do that for you:

    ```go
    select {
     case <-ctx.Done():
        break someLoop // or return
     default:
     }
    ```

* **Timeout**: If you want your conveyor to be killed if it's not done within a fixed time, then use:
    ```go
    cnv, err := NewConveyor()
    cnv.SetTimeout(timeout)
    ```
  But remember, if you want to use a custom context, you should set that first,
 and then set this timeout.
    ```go
     cnv, err := NewConveyor()
     cnv.SetCustomContext(ctx).SetTimeout(timeout)
     ```

* **Progress**: If you want to see what % of work is completed, 
use `EnableProgress()`to set expected runtime & to enable progress.

    ```go
     cnv, err := NewConveyor()
     cnv.EnableProgress(expectedDuration)
     ```

    To get the progress information, once conveyor is started, 
    just read from channel given by `conveyorInstance.Progress()`. 
    It decides progress based on a estimate that you provide as `expectedDuration`. 
    So if you have predicted 2 hour, it will be at 50% after an hour
    And if it's still not done after 2 hours, it will just wait at 99%, unless complete.


## Needs more work: Working with a distributed conveyor-based application

To go further, you need to know about Conveyor Life Cycle Handling. 
Conveyor was made to work with API based applications, that might be running on multiple servers, 
and we might need to retain the data even after a restart.
In such scenarios, you might actually want to store state, status messages, progress in some persistent manner, 
so that you can read that information on any server 
(even if a particular conveyor isn't running on that particular server) 

To create a conveyor based application that can work across a distributed system, 
you will need to provide an implementation of the below interface, 
where you can fetch/store data to/from your choice of database/cache 

```go
// LifeCycleHandler handles conveyor start/stop
type LifeCycleHandler interface {
	GetState() (string, error)
	GetStatusMsg() (string, error)
	UpdateStatusMsg(string) error
	GetProgress() (string, error)
	UpdateProgress(string) error

	MarkPreparing() error
	MarkStarted() error
	MarkToKill() error
	MarkKilled() error
	MarkFinished() error
	MarkError() error
}
```

In the implementation, that I use, in one of my applications, I store these details on a redis cluster.
In future, I do plan to simplify it a bit, and maybe, 
provide an in-built globas hash-based implementation for those who want to run a single server application.

For now, you can just use a map, to store these values in-memory, for single server applications.

Then, you can call `conveyorInstance.MarkCurrentState(x)` to change the state, 
where `x` can be one of the `conveyor.Status**` values.


It's currently a pretty new project, and I am open to new ideas and suggestions. 
Goes without saying, issues/contributions are welcomed as well.

#

