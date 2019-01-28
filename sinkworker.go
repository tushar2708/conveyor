package conveyor

import (
	"fmt"
	"log"

	"golang.org/x/sync/semaphore"
)

// SinkWorkerPool struct provides the worker pool infra for Sink interface
type SinkWorkerPool struct {
	*ConcreteNodeWorker
	inputChannel chan map[string]interface{}
}

// NewSinkWorkerPool creates a new SinkWorkerPool
func NewSinkWorkerPool(executor NodeExecutor, mode WorkerMode) NodeWorker {

	wCnt := executor.Count()
	if wCnt < 1 {
		wCnt = 1
	}

	swp := &SinkWorkerPool{
		ConcreteNodeWorker: &ConcreteNodeWorker{
			WPool: &WPool{
				Name: executor.GetName() + "_worker",
			},
			WorkerCount: wCnt,
			Mode:        mode,
			Executor:    executor,
		},
	}

	return swp
}

// CreateChannels creates channels for the sink worker
func (swp *SinkWorkerPool) CreateChannels(buffer int) {
	swp.inputChannel = make(chan map[string]interface{}, buffer)
}

// Start Sink Worker Pool
func (swp *SinkWorkerPool) Start(ctx CnvContext) error {
	if swp.Mode == WorkerModeTransaction {
		return swp.startTransactionMode(ctx)
	} else if swp.Mode == WorkerModeLoop {
		return swp.startLoopMode(ctx)
	} else {
		return ErrInvalidWorkerMode
	}
}

// startLoopMode SinkWorkerPool
func (swp *SinkWorkerPool) startLoopMode(ctx CnvContext) error {
	for i := 0; i < swp.WorkerCount; i++ {
		swp.Wg.Add(1)

		go func() {
			defer swp.Wg.Done()

			if err := swp.Executor.ExecuteLoop(ctx, swp.inputChannel, nil); err != nil {
				if err == ErrExecuteLoopNotImplemented {
					ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", swp.Executor.GetUniqueIdentifier()), err)
					log.Fatalf("Improper setup of Executor[%s], ExecuteLoop() method is required", swp.Executor.GetUniqueIdentifier())
				} else {
					return
				}
			}

		}()
	}
	return nil
}

// startTransactionMode starts SourceWorkerPool in transaction mode
func (swp *SinkWorkerPool) startTransactionMode(ctx CnvContext) error {
	swp.sem = semaphore.NewWeighted(int64(swp.WorkerCount))

workerLoop:
	for {

		select {
		case <-ctx.Done():
			break workerLoop
		default:
		}

		in, ok := <-swp.inputChannel
		if !ok {
			ctx.SendLog(0, fmt.Sprintf("Executor:[%s] sink's input channel closed", swp.Executor.GetUniqueIdentifier()), nil)
			break workerLoop
		}

		if err := swp.sem.Acquire(ctx, 1); err != nil {
			ctx.SendLog(0, fmt.Sprintf("Worker:[%s] for Executor:[%s] Failed to acquire semaphore", swp.Name, swp.Executor.GetUniqueIdentifier()), err)
			break
		}
		// fmt.Println("sink sem acquire 1")

		go func(data map[string]interface{}) {
			// defer fmt.Println("sink sem release 1")
			defer swp.sem.Release(1)
			if ok {
				_, err := swp.Executor.Execute(ctx, data)
				if err != nil {
					if err == ErrExecuteNotImplemented {
						ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", swp.Executor.GetUniqueIdentifier()), err)
						log.Fatalf("Improper setup of Executor[%s], Execute() method is required", swp.Executor.GetUniqueIdentifier())
					}
				}
			}
			return
		}(in)

	}

	return nil
}

// GetOutputChannel returns the output channel of Sink WorkerPool
func (swp *SinkWorkerPool) GetOutputChannel() (chan map[string]interface{}, error) {
	return nil, ErrOutputChanDoesNotExist
}

// GetInputChannel returns the input channel of Sink WorkerPool
func (swp *SinkWorkerPool) GetInputChannel() (chan map[string]interface{}, error) {
	return swp.inputChannel, nil
}

// SetInputChannel updates the input channel of Sink WorkerPool
func (swp *SinkWorkerPool) SetInputChannel(inChan chan map[string]interface{}) error {
	swp.inputChannel = inChan
	return nil
}

// SetOutputChannel updates the output channel of Sink WorkerPool
func (swp *SinkWorkerPool) SetOutputChannel(outChan chan map[string]interface{}) error {
	return ErrOutputChanDoesNotExist
}

// WorkerType returns the type of worker
func (swp *SinkWorkerPool) WorkerType() string {
	return SinkWorkerType
}

// WaitAndStop SinkWorkerPool
func (swp *SinkWorkerPool) WaitAndStop(ctx CnvContext) error {

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

	ctx.SendLog(3, fmt.Sprintf("Sink Worker:[%s] done, calling cleanup", swp.Name), nil)

	if cleanupErr := swp.Executor.CleanUp(); cleanupErr != nil {
		ctx.SendLog(0, fmt.Sprintf("Sink Worker:[%s] cleanup call failed. cleanupErr:[%v]", swp.Name, cleanupErr), nil)
	}
	return nil
}
