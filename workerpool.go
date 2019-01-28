package conveyor

import (
	"sync"

	"golang.org/x/sync/semaphore"
)

const (
	// SourceWorkerType constant
	SourceWorkerType = "SOURCE_WORKER"
	// FetchWorkerType constant
	FetchWorkerType = "FETCH_WORKER"
	// SinkWorkerType constant
	SinkWorkerType = "SINK_WORKER"
	// PlumbingWorkerType constant
	PlumbingWorkerType = "PLUMBING_WORKER"
)

// WorkerMode decides if worker would run in loop mode or single transaction mode
type WorkerMode uint8

const (
	// WorkerModeTransaction is the worker mode in which executor just needs to implement Execute(ctx) method
	// and doesn't need to handle worker's channel or shutdown of worker.
	// But executor should still monitor ctx.Done() to shutdown or cleanup/close any files/connections, etc that it opens.
	// This one helps keep the executor code leaner & simple
	// This mode is useful for most of the cases, including executors that do file I/O, database lookup, remote API call
	WorkerModeTransaction = WorkerMode(iota + 10)

	// WorkerModeLoop is the worker mode in which executor just needs to implement ExecuteLoop(ctx, inChan, outChan) method
	// and has to handle the copying of data from/to channels (except, closing them), executor will also need to ensure
	// that it monitors ctx.Done() to shutdown worker, in case of any error.
	// Needs more code, use only if you are ready to peek into how it works.
	// Some use cases are, where you can't fetch data on-demand with a function call. Eg. Running an NPI server as source
	WorkerModeLoop
)

// WPool to run different nodes of comex graph
type WPool struct {
	Name string
	// Executor    common.NodeExecutor
	sigChannel chan interface{}
	Wg         sync.WaitGroup
	sem        *semaphore.Weighted
}

// ConcreteNodeWorker to run different nodes of comex graph
type ConcreteNodeWorker struct {
	*WPool
	WorkerCount int
	Mode        WorkerMode
	Executor    NodeExecutor
}

// ConcreteJointWorker to run different nodes of comex graph
type ConcreteJointWorker struct {
	*WPool
	Executor JointExecutor
}

// NodeWorker interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type NodeWorker interface {
	Start(ctx CnvContext) error
	WaitAndStop(ctx CnvContext) error
	CreateChannels(int)
	WorkerType() string
	SetInputChannel(chan map[string]interface{}) error
	SetOutputChannel(chan map[string]interface{}) error
	GetInputChannel() (chan map[string]interface{}, error)
	GetOutputChannel() (chan map[string]interface{}, error)
}

// JointWorker interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type JointWorker interface {
	Start(ctx CnvContext) error
	WaitAndStop() error
	CreateChannels(int)
	SetInputChannels([]chan map[string]interface{}) error
	SetOutputChannels([]chan map[string]interface{}) error
	GetInputChannels() ([]chan map[string]interface{}, error)
	GetOutputChannels() ([]chan map[string]interface{}, error)
	AddInputChannel(chan map[string]interface{}) error
	AddOutputChannel(chan map[string]interface{}) error
}

// Start the worker
func (wp *ConcreteNodeWorker) Start() {
	for i := 0; i < wp.Executor.Count(); i++ {
		wp.Wg.Add(1)
		go wp.run()
	}
}

// CreateChannels creates channels for the worker
func (wp *ConcreteNodeWorker) CreateChannels(buffer int) {}

// Start the worker
func (wp *ConcreteJointWorker) Start() {
	for i := 0; i < wp.Executor.Count(); i++ {
		wp.Wg.Add(1)
		go wp.run()
	}
}

// Wait for worker to finish
func (wp *WPool) Wait() {
	wp.Wg.Wait()
}

func (wp *WPool) run() {
	wp.Wg.Done()
}
