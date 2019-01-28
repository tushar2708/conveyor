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
	nextWorkerCount int
	outputChannel   chan map[string]interface{}
}

// NewSourceWorkerPool creates a new SourceWorkerPool
func NewSourceWorkerPool(executor NodeExecutor, mode WorkerMode) NodeWorker {

	wCnt := executor.Count()
	if wCnt < 1 {
		wCnt = 1
	}

	swp := &SourceWorkerPool{
		ConcreteNodeWorker: &ConcreteNodeWorker{
			WPool: &WPool{
				Name: executor.GetName() + "_worker",
			},
			WorkerCount: wCnt,
			Mode:        mode,
			Executor:    executor,
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
func (swp *SourceWorkerPool) Start(ctx CnvContext) error {
	if swp.Mode == WorkerModeTransaction {
		return swp.startTransactionMode(ctx)
	} else if swp.Mode == WorkerModeLoop {
		return swp.startLoopMode(ctx)
	} else {
		return ErrInvalidWorkerMode
	}
}

// startLoopMode SourceWorkerPool
func (swp *SourceWorkerPool) startLoopMode(ctx CnvContext) error {

	for i := 0; i < swp.WorkerCount; i++ {
		swp.Wg.Add(1)
		// fmt.Println("src wg add 1")
		go func() {
			// defer fmt.Println("src wg done 1")
			defer swp.Wg.Done()
			if err := swp.Executor.ExecuteLoop(ctx, nil, swp.outputChannel); err != nil {
				if err == ErrExecuteLoopNotImplemented {
					ctx.SendLog(0, fmt.Sprintf("Executor:[%s] ", swp.Executor.GetUniqueIdentifier()), err)

					log.Fatalf("Improper setup of Executor[%s], ExecuteLoop() method is required", swp.Executor.GetName())
				}
				return
			}
		}()
	}
	return nil
}

// startTransactionMode starts SourceWorkerPool in transaction mode
func (swp *SourceWorkerPool) startTransactionMode(ctx CnvContext) error {

	swp.sem = semaphore.NewWeighted(int64(swp.WorkerCount))

	workerDone := false
	doneMutex := new(sync.RWMutex)

workerLoop:
	for {

		doneMutex.RLock()
		if workerDone == true {
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
			defer swp.sem.Release(1)
			out, err := swp.Executor.Execute(ctx, nil)
			if err == nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				swp.outputChannel <- out
			} else if err == ErrExecuteNotImplemented {
				ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", swp.Executor.GetUniqueIdentifier()), err)
				log.Fatalf("Improper setup of Executor[%s], Execute() method is required", swp.Executor.GetUniqueIdentifier())
			} else if err == ErrSourceExhausted {
				ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", swp.Executor.GetUniqueIdentifier()), err)
				doneMutex.Lock()
				workerDone = true
				doneMutex.Unlock()
				ctx.Cancel()
				return
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
func (swp *SourceWorkerPool) WaitAndStop(ctx CnvContext) error {

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if swp.Mode == WorkerModeTransaction {
		if err := swp.sem.Acquire(ctx, int64(swp.WorkerCount)); err != nil {
			ctx.SendLog(0, fmt.Sprintf("Worker:[%s] for Executor:[%s] Failed to acquire semaphore", swp.Name, swp.Executor.GetUniqueIdentifier()), err)
		}
	} else {
		swp.Wg.Wait()
	}
	ctx.SendLog(3, fmt.Sprintf("Source Worker:[%s] done, calling cleanup", swp.Name), nil)

	if cleanupErr := swp.Executor.CleanUp(); cleanupErr != nil {
		ctx.SendLog(0, fmt.Sprintf("Source Worker:[%s] cleanup call failed. cleanupErr:[%v]", swp.Name, cleanupErr), nil)
	}
	// fmt.Println("going to close src out channel")
	close(swp.outputChannel)
	return nil
}
