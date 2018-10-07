package conveyor

import (
	"errors"
	"sync"
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

var (
	// ErrInputChanDoesNotExist error
	ErrInputChanDoesNotExist = errors.New("input channel doesn't exist for this node")
	// ErrOutputChanDoesNotExist error
	ErrOutputChanDoesNotExist = errors.New("output channel doesn't exist for this node")
)

// WPool to run different nodes of comex graph
type WPool struct {
	Name string
	// Executor    common.NodeExecutor
	sigChannel chan interface{}
	Wg         sync.WaitGroup
}

// ConcreteNodeWorker to run different nodes of comex graph
type ConcreteNodeWorker struct {
	WPool
	Executor NodeExecutor
}

// ConcreteJointWorker to run different nodes of comex graph
type ConcreteJointWorker struct {
	WPool
	Executor JointExecutor
}

// NodeWorker interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type NodeWorker interface {
	Start(ctx *CnvContext) error
	WaitAndStop() error
	CreateChannels(int)
	WorkerType() string
	SetInputChannel(chan map[string]interface{}) error
	SetOutputChannel(chan map[string]interface{}) error
	GetInputChannel() (chan map[string]interface{}, error)
	GetOutputChannel() (chan map[string]interface{}, error)
}

// JointWorker interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type JointWorker interface {
	Start(ctx *CnvContext) error
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
