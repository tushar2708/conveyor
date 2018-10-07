package conveyor

import (
	"log"

	"github.com/sudersen/glog"
	"golang.org/x/sync/semaphore"
)

// // FetchExecutor interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
// type FetchExecutor interface {
// 	FetchAndSend(inputChannel chan interface{}, outputChannel []chan interface{})
// 	Start()
// 	Stop()
// 	GetPool() *FetchWorkerPool
// }

// FetchWorkerPool struct provides the worker pool infra for Fetch interface
type FetchWorkerPool struct {
	ConcreteNodeWorker
	nextWorkerCount int
	inputChannel    chan map[string]interface{}
	outputChannel   chan map[string]interface{}
}

// FetchNode structue
type FetchNode struct {
	Pool *FetchWorkerPool
}

// NewFetchWorkerPool creates a new FetchWorkerPool
func NewFetchWorkerPool(executor NodeExecutor, mode WorkerMode) NodeWorker {

	fwp := &FetchWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: executor.GetName() + "_worker",
			},
			Mode:     mode,
			Executor: executor,
		},
	}

	return fwp
}

// CreateChannels creates channels for the fetch worker
func (fwp *FetchWorkerPool) CreateChannels(buffer int) {
	fwp.inputChannel = make(chan map[string]interface{}, buffer)
}

// GetInputChannel returns the input channel of Fetch WorkerPool
func (fwp *FetchWorkerPool) GetInputChannel() (chan map[string]interface{}, error) {
	return fwp.inputChannel, nil
}

// GetOutputChannel returns the output channel of Fetch WorkerPool
func (fwp *FetchWorkerPool) GetOutputChannel() (chan map[string]interface{}, error) {
	return fwp.outputChannel, nil
}

// SetInputChannel updates the input channel of Source WorkerPool
func (fwp *FetchWorkerPool) SetInputChannel(inChan chan map[string]interface{}) error {
	fwp.inputChannel = inChan
	return nil
}

// SetOutputChannel updates the output channel of Source WorkerPool
func (fwp *FetchWorkerPool) SetOutputChannel(outChan chan map[string]interface{}) error {
	fwp.outputChannel = outChan
	return nil
}

// Start Fetch Worker Pool
func (fwp *FetchWorkerPool) Start(ctx *CnvContext) error {
	if fwp.Mode == WorkerModeTransaction {
		return fwp.startTransactionMode(ctx)
	} else if fwp.Mode == WorkerModeLoop {
		return fwp.startLoopMode(ctx)
	} else {
		return ErrInvalidWorkerMode
	}
}

// startLoopMode FetchWorkerPool
func (fwp *FetchWorkerPool) startLoopMode(ctx *CnvContext) error {
	for i := 0; i < fwp.Executor.Count(); i++ {
		fwp.Wg.Add(1)
		go func() {
			defer fwp.Wg.Done()

			if err := fwp.Executor.ExecuteLoop(ctx, fwp.inputChannel, fwp.outputChannel); err != nil {
				if err == ErrExecuteLoopNotImplemented {
					glog.V(3).Infof("Executor:[%s], Err:[%s]", fwp.Executor.GetName(), err.Error())
					log.Fatalf("Improper setup of Executor[%s], ExecuteLoop() method is required", fwp.Executor.GetName())
				} else {
					return
				}
			}
		}()
	}
	return nil
}

// startTransactionMode starts FetchWorkerPool in transaction mode
func (fwp *FetchWorkerPool) startTransactionMode(ctx *CnvContext) error {
	wCnt := fwp.Executor.Count()
	sem := semaphore.NewWeighted(int64(wCnt))

workerLoop:
	for {

		select {
		case <-ctx.Done():
			break workerLoop
		default:
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
			break
		}

		go func() {
			defer sem.Release(1)
			in, ok := <-fwp.inputChannel
			if ok {
				out, err := fwp.Executor.Execute(ctx, in)
				if err == nil {
					fwp.outputChannel <- out
				} else if err == ErrExecuteNotImplemented {
					glog.V(3).Infof("Executor:[%s], Err:[%s]", fwp.Executor.GetName(), err.Error())
					log.Fatalf("Improper setup of Executor[%s], Execute() method is required", fwp.Executor.GetName())
				}
			}
			return
		}()

	}

	return nil
}

// WorkerType returns the type of worker
func (fwp *FetchWorkerPool) WorkerType() string {
	return FetchWorkerType
}

// WaitAndStop FetchWorkerPool
func (fwp *FetchWorkerPool) WaitAndStop(ctx *CnvContext) error {

	if fwp.Mode == WorkerModeTransaction {
		if err := fwp.sem.Acquire(ctx, int64(fwp.Executor.Count())); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
		}
	} else {
		fwp.Wg.Wait()
	}

	fwp.Executor.CleanUp()
	close(fwp.outputChannel)
	return nil
}
