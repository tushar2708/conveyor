# conveyor

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Ftushar2708%2Fconveyor.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Ftushar2708%2Fconveyor?ref=badge_shield)

[![GitHub license](https://img.shields.io/github/license/mashape/apistatus.svg)]()

[![HitCount](http://hits.dwyl.io/tushar2708/conveyor.svg)](http://hits.dwyl.io/tushar2708/conveyor)

[![Maintainability](https://api.codeclimate.com/v1/badges/e6c8164f8cbf98490fe8/maintainability)](https://codeclimate.com/github/tushar2708/conveyor/maintainability)

[![Test Coverage](https://api.codeclimate.com/v1/badges/e6c8164f8cbf98490fe8/test_coverage)](https://codeclimate.com/github/tushar2708/conveyor/test_coverage)

A go pipeline management library, supporting concurrent pipelines, with multiple nodes and joints.

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

There are these 2 interfaces that you need to implement in your own types, for Nodes & Joints, respectively:

```go
// NodeExecutor interface is the interface that you need to implement by your own types of nodes
type NodeExecutor interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error
	Execute(ctx CnvContext, inData map[string]interface{}) (map[string]interface{}, error)
	Count() int
	CleanUp() error
}

// JointExecutor interface is the interface that you need to implement by your own types of joints
type JointExecutor interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChan []chan map[string]interface{}, outChan []chan map[string]interface{}) error
	Count() int
	InputCount() int
	OutputCount() int
}
```

* `GetName()` returns a string to represent the name of the executor.

* `GetUniqueIdentifier()` returns a unique-identifier to recognise the executor (you may just return the name).

* `Count()` decides the concurrency you want to have for your node.

* `CleanUp()` is called once node's job is completed. To do any cleanup activity 
(like closing a file/database connection, or to wait till your other go-routines finish)


If you don't want to implement all of these methods, it makes sense. 
There's a struct `conveyor.ConcreteNodeExecutor` that gives default implementations of these 4 methods.
You can get up and running without them, but for an actual application, 
where hopefully, you will need concurrency, don't forget to return "something > 1: from `Count()`.
it's default value is 1. Also, always `Cleanup()` after yourself.  
 
 But, you must implement one of the below 2 methods, based on what you want your node to do.
 Their default implementation just returns `ErrExecuteNotImplemented` error.

* Execute() receives a `map[string]interface{}` (inData) as input, and returns a `map[string]interface{}` as output.
This is the method that you must implement based on what you want your node to do. This is the method gets called, 
if you have added your node to work in "Transaction Mode" (check example for more on that). In most cases, you can just
get the most out of this method. Here we create a new Go-routine for each request, and number of go-routines running 
at a given time, is decided by `Count()`. If your Execute() for source node returns an error `ErrSourceExhausted`, 
conveyor will assume that it's time to finish up, and will stop reading any more data, 
and will gracefully shutdown after processing already read data. Generally, if any node returns a non-nil error, 
that particular unit of data would be dropped, and won't be sent to the next node.

* ExecuteLoop() gets called if your Executor has been added to work in "Loop Mode". 
It receives 2 channels (inChan & outChan) having `map[string]interface{}`.
You will be needed to run a for loop, to read inputs from `inChan`, and write it to `outChan`
It is supposed to be a blocking function, but the loop should exit once you are done. 
If there are 10 instances of this function running, and all of them return, 
Conveyor will take that as a signal to shutdown, the node, and the ones that come next to it.
As a rule of thumb, always let your source dictate when to finish up, all other will automatically follow it's lead.

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

