package conveyor

import (
	"fmt"
	"log"
	"runtime/debug"
	"sync"

	"golang.org/x/sync/semaphore"
)

const (
	// WorkerTypeSource constant
	WorkerTypeSource = "SOURCE_WORKER"
	// WorkerTypeOperation constant
	WorkerTypeOperation = "OPERATION_WORKER"
	// WorkerTypeSink constant
	WorkerTypeSink = "SINK_WORKER"
	// WorkerTypeJoint constant
	WorkerTypeJoint = "JOINT_WORKER"
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
	// Some use cases are, where you can't fetch data on-demand with a function call. Eg. Running an API server as source
	WorkerModeLoop
)

// WPool to run different nodes of comex graph
type WPool struct {
	Name string
	Wg   sync.WaitGroup
	sem  *semaphore.Weighted
}

// ConcreteNodeWorker to run different nodes
type ConcreteNodeWorker struct {
	*WPool
	WorkerCount int
	Mode        WorkerMode
	Executor    NodeExecutor
}

// ConcreteJointWorker to run different joints
type ConcreteJointWorker struct {
	*WPool
	Executor JointExecutor
}

// NodeWorker interface binds to nodes that have the capability to fetch intermediate data, and forward it to next node
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

func newConcreteNodeWorker(executor NodeExecutor, mode WorkerMode) *ConcreteNodeWorker {

	wCnt := executor.Count()
	if wCnt < 1 {
		wCnt = 1
	}
	cnw := &ConcreteNodeWorker{
		WPool: &WPool{
			Name: executor.GetName() + "_worker",
		},
		WorkerCount: wCnt,
		Mode:        mode,
		Executor:    executor,
	}

	return cnw
}

// Start the worker
func (cnw *ConcreteNodeWorker) Start() {
	for i := 0; i < cnw.Executor.Count(); i++ {
		cnw.Wg.Add(1)
		go cnw.run()
	}
}

// startLoopMode starts ConcreteNodeWorker in loop mode
func (cnw *ConcreteNodeWorker) startLoopMode(ctx CnvContext, inputChannel chan map[string]interface{},
	outChannel chan map[string]interface{}) error {

	for i := 0; i < cnw.WorkerCount; i++ {
		cnw.Wg.Add(1)
		go func() {
			defer cnw.recovery(ctx, "ConcreteNodeWorker")
			defer cnw.Wg.Done()
			if err := cnw.Executor.ExecuteLoop(ctx, inputChannel, outChannel); err != nil {
				if err == ErrExecuteLoopNotImplemented {
					ctx.SendLog(0, fmt.Sprintf("Executor:[%s] ", cnw.Executor.GetUniqueIdentifier()), err)

					log.Fatalf("Improper setup of Executor[%s], ExecuteLoop() method is required",
						cnw.Executor.GetName())
				}
				return
			}
		}()
	}

	return nil
}

// CreateChannels creates channels for the worker
func (cnw *ConcreteNodeWorker) CreateChannels(buffer int) {}

// WaitAndStop ConcreteNodeWorker
func (cnw *ConcreteNodeWorker) WaitAndStop(ctx CnvContext) error {

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if cnw.Mode == WorkerModeTransaction {
		if err := cnw.sem.Acquire(ctx, int64(cnw.WorkerCount)); err != nil {
			ctx.SendLog(0, fmt.Sprintf("Worker:[%s] for Executor:[%s] Failed to acquire semaphore",
				cnw.Name, cnw.Executor.GetUniqueIdentifier()), err)
		}
	} else {
		cnw.Wg.Wait()
	}
	ctx.SendLog(3, fmt.Sprintf("Worker:[%s] done, calling cleanup", cnw.Name), nil)

	if cleanupErr := cnw.Executor.CleanUp(); cleanupErr != nil {
		ctx.SendLog(0, fmt.Sprintf("Worker:[%s] cleanup call failed. cleanupErr:[%v]",
			cnw.Name, cleanupErr), nil)
	}
	return nil
}

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

func (cnw *ConcreteNodeWorker) recovery(ctx CnvContext, caller string) {
	if r := recover(); r != nil {
		ctx.SendLog(0, fmt.Sprintf("Worker:[%s] for Executor:[%s] recovered:[%v] caller:[%s]",
			cnw.Name, cnw.Executor.GetUniqueIdentifier(), r, caller), nil)

		fmt.Println("recovered:", r, caller)
		debug.PrintStack()
	}
}
