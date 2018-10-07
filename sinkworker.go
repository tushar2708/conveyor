package conveyor

import (
	"log"

	"github.com/sudersen/glog"
	"golang.org/x/sync/semaphore"
)

// SinkWorkerPool struct provides the worker pool infra for Sink interface
type SinkWorkerPool struct {
	ConcreteNodeWorker
	inputChannel chan map[string]interface{}
}

// NewSinkWorkerPool creates a new SinkWorkerPool
func NewSinkWorkerPool(executor NodeExecutor, mode WorkerMode) NodeWorker {

	swp := &SinkWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: executor.GetName() + "_worker",
			},
			Mode:     mode,
			Executor: executor,
		},
	}

	return swp
}

// CreateChannels creates channels for the sink worker
func (swp *SinkWorkerPool) CreateChannels(buffer int) {
	swp.inputChannel = make(chan map[string]interface{}, buffer)
}

// Start Sink Worker Pool
func (swp *SinkWorkerPool) Start(ctx *CnvContext) error {
	if swp.Mode == WorkerModeTransaction {
		return swp.startTransactionMode(ctx)
	} else if swp.Mode == WorkerModeLoop {
		return swp.startLoopMode(ctx)
	} else {
		return ErrInvalidWorkerMode
	}
}

// startLoopMode SinkWorkerPool
func (swp *SinkWorkerPool) startLoopMode(ctx *CnvContext) error {
	for i := 0; i < swp.Executor.Count(); i++ {
		swp.Wg.Add(1)

		go func() {
			defer swp.Wg.Done()
			swp.Executor.ExecuteLoop(ctx, swp.inputChannel, nil)

			if err := swp.Executor.ExecuteLoop(ctx, swp.inputChannel, nil); err != nil {
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
func (swp *SinkWorkerPool) startTransactionMode(ctx *CnvContext) error {
	wCnt := swp.Executor.Count()
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
			in, ok := <-swp.inputChannel
			if ok {
				_, err := swp.Executor.Execute(ctx, in)
				if err != nil {
					if err == ErrExecuteNotImplemented {
						glog.V(3).Infof("Executor:[%s], Err:[%s]", swp.Executor.GetName(), err.Error())
						log.Fatalf("Improper setup of Executor[%s], Execute() method is required", swp.Executor.GetName())
					}
				}
			}
			return
		}()

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
func (swp *SinkWorkerPool) WaitAndStop(ctx *CnvContext) error {

	if swp.Mode == WorkerModeTransaction {
		if err := swp.sem.Acquire(ctx, int64(swp.Executor.Count())); err != nil {
			log.Printf("Failed to acquire semaphore: %v", err)
		}
	} else {
		swp.Wg.Wait()
	}

	swp.Executor.CleanUp()
	return nil
}
