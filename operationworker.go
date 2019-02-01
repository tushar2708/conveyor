package conveyor

import (
	"fmt"
	"log"

	"golang.org/x/sync/semaphore"
)


// OperationWorkerPool struct provides the worker pool infra for Operation interface
type OperationWorkerPool struct {
	*ConcreteNodeWorker
	nextWorkerCount int
	inputChannel    chan map[string]interface{}
	outputChannel   chan map[string]interface{}
}

// OperationNode structue
type OperationNode struct {
	Pool *OperationWorkerPool
}

// NewOperationWorkerPool creates a new OperationWorkerPool
func NewOperationWorkerPool(executor NodeExecutor, mode WorkerMode) NodeWorker {

	wCnt := executor.Count()
	if wCnt < 1 {
		wCnt = 1
	}

	fwp := &OperationWorkerPool{
		ConcreteNodeWorker: &ConcreteNodeWorker{
			WPool: &WPool{
				Name: executor.GetName() + "_worker",
			},
			WorkerCount: wCnt,
			Mode:        mode,
			Executor:    executor,
		},
	}

	return fwp
}

// CreateChannels creates channels for the Operation WorkerPool
func (fwp *OperationWorkerPool) CreateChannels(buffer int) {
	fwp.inputChannel = make(chan map[string]interface{}, buffer)
}

// GetInputChannel returns the input channel of Operation WorkerPool
func (fwp *OperationWorkerPool) GetInputChannel() (chan map[string]interface{}, error) {
	return fwp.inputChannel, nil
}

// GetOutputChannel returns the output channel of Operation WorkerPool
func (fwp *OperationWorkerPool) GetOutputChannel() (chan map[string]interface{}, error) {
	return fwp.outputChannel, nil
}

// SetInputChannel updates the input channel of Operation WorkerPool
func (fwp *OperationWorkerPool) SetInputChannel(inChan chan map[string]interface{}) error {
	fwp.inputChannel = inChan
	return nil
}

// SetOutputChannel updates the output channel of Operation WorkerPool
func (fwp *OperationWorkerPool) SetOutputChannel(outChan chan map[string]interface{}) error {
	fwp.outputChannel = outChan
	return nil
}

// Start Operation Worker Pool
func (fwp *OperationWorkerPool) Start(ctx CnvContext) error {
	if fwp.Mode == WorkerModeTransaction {
		return fwp.startTransactionMode(ctx)
	} else if fwp.Mode == WorkerModeLoop {
		return fwp.startLoopMode(ctx)
	} else {
		return ErrInvalidWorkerMode
	}
}

// startLoopMode OperationWorkerPool
func (fwp *OperationWorkerPool) startLoopMode(ctx CnvContext) error {

	for i := 0; i < fwp.WorkerCount; i++ {
		fwp.Wg.Add(1)
		go func() {
			defer fwp.Wg.Done()

			if err := fwp.Executor.ExecuteLoop(ctx, fwp.inputChannel, fwp.outputChannel); err != nil {
				if err == ErrExecuteLoopNotImplemented {
					ctx.SendLog(0, fmt.Sprintf("Executor:[%s] ", fwp.Executor.GetUniqueIdentifier()), err)
					log.Fatalf("Improper setup of Executor[%s], ExecuteLoop() method is required", fwp.Executor.GetName())
				} else {
					return
				}
			}
		}()
	}
	return nil
}

// startTransactionMode starts OperationWorkerPool in transaction mode
func (fwp *OperationWorkerPool) startTransactionMode(ctx CnvContext) error {

	fwp.sem = semaphore.NewWeighted(int64(fwp.WorkerCount))

workerLoop:
	for {

		select {
		case <-ctx.Done():
			break workerLoop
		default:
		}

		in, ok := <-fwp.inputChannel
		if !ok {
			ctx.SendLog(0, fmt.Sprintf("Executor:[%s] Operation's input channel closed", fwp.Executor.GetUniqueIdentifier()), nil)
			break workerLoop
		}

		if err := fwp.sem.Acquire(ctx, 1); err != nil {
			ctx.SendLog(0, fmt.Sprintf("Executor:[%s], sem acquire failed", fwp.Executor.GetUniqueIdentifier()), err)
			break workerLoop
		}
		// fmt.Println("Operation sem acquire 1")

		go func(data map[string]interface{}) {
			// defer fmt.Println("Operation sem release 1")
			defer fwp.sem.Release(1)

			out, err := fwp.Executor.Execute(ctx, data)
			if err == nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				fwp.outputChannel <- out
			} else if err == ErrExecuteNotImplemented {
				ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", fwp.Executor.GetUniqueIdentifier()), err)
				log.Fatalf("Improper setup of Executor[%s], Execute() method is required", fwp.Executor.GetUniqueIdentifier())
			}
			return
		}(in)

	}

	return nil
}

// WorkerType returns the type of worker
func (fwp *OperationWorkerPool) WorkerType() string {
	return WorkerTypeOperation
}

// WaitAndStop OperationWorkerPool
func (fwp *OperationWorkerPool) WaitAndStop(ctx CnvContext) error {

	_ = fwp.ConcreteNodeWorker.WaitAndStop(ctx)

	close(fwp.outputChannel)
	return nil
}
