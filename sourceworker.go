package conveyor

import (
	"fmt"
	"log"
	"sync"

	"golang.org/x/sync/semaphore"
)

// SourceWorkerPool struct provides the worker pool infra for Source interface
type SourceWorkerPool struct {
	*ConcreteNodeWorker
	outputChannel chan any
}

// NewSourceWorkerPool creates a new SourceWorkerPool
func NewSourceWorkerPool(executor nodeExecutor, mode WorkerMode) NodeWorker {

	cnw := newConcreteNodeWorker(executor, mode)
	swp := &SourceWorkerPool{ConcreteNodeWorker: cnw}

	return swp
}

// GetOutputChannel returns the output channel of Source WorkerPool
func (swp *SourceWorkerPool) GetOutputChannel() (chan any, error) {
	return swp.outputChannel, nil
}

// GetInputChannel returns the input channel of Source WorkerPool
func (swp *SourceWorkerPool) GetInputChannel() (chan any, error) {
	return nil, ErrInputChanDoesNotExist
}

// SetInputChannel updates the input channel of Source WorkerPool
func (swp *SourceWorkerPool) SetInputChannel(inChan chan any) error {
	return ErrInputChanDoesNotExist
}

// SetOutputChannel updates the output channel of Source WorkerPool
func (swp *SourceWorkerPool) SetOutputChannel(outChan chan any) error {
	swp.outputChannel = outChan
	return nil
}

// Start Source Worker Pool
func (swp *SourceWorkerPool) Start(ctx CnvContext) error {
	switch swp.Mode {
	case WorkerModeTransaction:
		return swp.startTransactionMode(ctx)
	case WorkerModeLoop:
		return swp.startLoopMode(ctx)
	default:
		return ErrInvalidWorkerMode
	}
}

// startLoopMode SourceWorkerPool
func (swp *SourceWorkerPool) startLoopMode(ctx CnvContext) error {

	return swp.ConcreteNodeWorker.startLoopMode(ctx, nil, swp.outputChannel)

}

// startTransactionMode starts SourceWorkerPool in transaction mode
func (swp *SourceWorkerPool) startTransactionMode(ctx CnvContext) error {

	swp.sem = semaphore.NewWeighted(int64(swp.WorkerCount))

	workerDone := false
	doneMutex := new(sync.RWMutex)

workerLoop:
	for {

		doneMutex.RLock()
		if workerDone {
			doneMutex.RUnlock()
			break workerLoop
		}
		doneMutex.RUnlock()

		select {
		case <-ctx.Done():
			break workerLoop
		default:
		}

		if err := swp.sem.Acquire(ctx, 1); err != nil {
			ctx.SendLog(0, fmt.Sprintf("Worker:[%s] for Executor:[%s] Failed to acquire semaphore", swp.Name, swp.Executor.GetUniqueIdentifier()), err)
			break workerLoop
		}

		go func() {
			defer swp.recovery(ctx, "SourceWorkerPool")
			defer swp.sem.Release(1)
			outData, err := swp.Executor.executeUntyped(ctx, nil)
			switch err {
			case nil:
				select {
				case <-ctx.Done():
					return
				default:
				}
				swp.outputChannel <- outData
			case ErrExecuteNotImplemented:
				ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", swp.Executor.GetUniqueIdentifier()), err)
				log.Fatalf("Improper setup of Executor[%s], Execute() method is required",
					swp.Executor.GetUniqueIdentifier())
			case ErrSourceExhausted:
				ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", swp.Executor.GetUniqueIdentifier()), err)
				doneMutex.Lock()
				workerDone = true
				doneMutex.Unlock()
				return
			default:
				ctx.SendLog(2, fmt.Sprintf("Worker:[%s] for Executor:[%s] Execute() Call Failed.",
					swp.Name, swp.Executor.GetUniqueIdentifier()), err)
			}
		}()

	}

	return nil
}

// WorkerType returns the type of worker
func (swp *SourceWorkerPool) WorkerType() string {
	return WorkerTypeSource
}

// WaitAndStop SourceWorkerPool
func (swp *SourceWorkerPool) WaitAndStop(ctx CnvContext) error {

	_ = swp.ConcreteNodeWorker.WaitAndStop(ctx)

	close(swp.outputChannel)
	return nil
}
