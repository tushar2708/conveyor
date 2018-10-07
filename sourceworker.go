package conveyor

import (
	"log"

	"github.com/sudersen/glog"
	"golang.org/x/sync/semaphore"
)

// SourceWorkerPool struct provides the worker pool infra for Source interface
type SourceWorkerPool struct {
	ConcreteNodeWorker
	nextWorkerCount int
	outputChannel   chan map[string]interface{}
}

// NewSourceWorkerPool creates a new SourceWorkerPool
func NewSourceWorkerPool(executor NodeExecutor, mode WorkerMode) NodeWorker {

	swp := &SourceWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: executor.GetName() + "_worker",
			},
			Mode:     mode,
			Executor: executor,
		},
	}

	swp.sigChannel = make(chan interface{}, 1)
	return swp
}

// GetOutputChannel returns the output channel of Source WorkerPool
func (swp *SourceWorkerPool) GetOutputChannel() (chan map[string]interface{}, error) {
	return swp.outputChannel, nil
}

// GetInputChannel returns the input channel of Source WorkerPool
func (swp *SourceWorkerPool) GetInputChannel() (chan map[string]interface{}, error) {
	return nil, ErrInputChanDoesNotExist
}

// SetInputChannel updates the input channel of Source WorkerPool
func (swp *SourceWorkerPool) SetInputChannel(inChan chan map[string]interface{}) error {
	return ErrInputChanDoesNotExist
}

// SetOutputChannel updates the output channel of Source WorkerPool
func (swp *SourceWorkerPool) SetOutputChannel(outChan chan map[string]interface{}) error {
	swp.outputChannel = outChan
	return nil
}

// Start Source Worker Pool
func (swp *SourceWorkerPool) Start(ctx *CnvContext) error {
	if swp.Mode == WorkerModeTransaction {
		return swp.startTransactionMode(ctx)
	} else if swp.Mode == WorkerModeLoop {
		return swp.startLoopMode(ctx)
	} else {
		return ErrInvalidWorkerMode
	}
}

// startLoopMode SourceWorkerPool
func (swp *SourceWorkerPool) startLoopMode(ctx *CnvContext) error {
	for i := 0; i < swp.Executor.Count(); i++ {
		swp.Wg.Add(1)
		go func() {
			defer swp.Wg.Done()
			if err := swp.Executor.ExecuteLoop(ctx, nil, swp.outputChannel); err != nil {
				if err == ErrExecuteLoopNotImplemented {
					glog.V(3).Infof("Executor:[%s], Err:[%s]", swp.Executor.GetName(), err.Error())
					log.Fatalf("Improper setup of Executor[%s], ExecuteLoop() method is required", swp.Executor.GetName())
				} else {
					return
				}
			}
		}()
	}
	return nil
}

// startTransactionMode starts SourceWorkerPool in transaction mode
func (swp *SourceWorkerPool) startTransactionMode(ctx *CnvContext) error {
	wCnt := swp.Executor.Count()
	swp.sem = semaphore.NewWeighted(int64(wCnt))

workerLoop:
	for {

		select {
		case <-ctx.Done():
			break workerLoop
		default:
		}

		if err := swp.sem.Acquire(ctx, 1); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
			break
		}

		go func() {
			defer swp.sem.Release(1)
			out, err := swp.Executor.Execute(ctx, nil)
			if err == nil {
				swp.outputChannel <- out
			} else if err == ErrExecuteNotImplemented {
				glog.V(3).Infof("Executor:[%s], Err:[%s]", swp.Executor.GetName(), err.Error())
				log.Fatalf("Improper setup of Executor[%s], Execute() method is required", swp.Executor.GetName())
			}
			return
		}()

	}

	return nil
}

// WorkerType returns the type of worker
func (swp *SourceWorkerPool) WorkerType() string {
	return SourceWorkerType
}

// WaitAndStop SourceWorkerPool
func (swp *SourceWorkerPool) WaitAndStop(ctx *CnvContext) error {
	if swp.Mode == WorkerModeTransaction {
		if err := swp.sem.Acquire(ctx, int64(swp.Executor.Count())); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
		}
	} else {
		swp.Wg.Wait()
	}
	swp.Executor.CleanUp()
	close(swp.outputChannel)
	return nil
}
