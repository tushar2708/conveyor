package conveyor

import (
	"fmt"
	"log"

	"golang.org/x/sync/semaphore"
)

// OperationWorkerPool struct provides the worker pool infra for Operation interface
type OperationWorkerPool struct {
	*ConcreteNodeWorker
	inputChannel  chan any
	outputChannel chan any
}

// OperationNode structue
type OperationNode struct {
	Pool *OperationWorkerPool
}

// NewOperationWorkerPool creates a new OperationWorkerPool
func NewOperationWorkerPool(executor nodeExecutor, mode WorkerMode) NodeWorker {

	cnw := newConcreteNodeWorker(executor, mode)
	fwp := &OperationWorkerPool{ConcreteNodeWorker: cnw}

	return fwp
}

// CreateChannels creates channels for the Operation WorkerPool
func (fwp *OperationWorkerPool) CreateChannels(buffer int) {
	fwp.inputChannel = make(chan any, buffer)
}

// GetInputChannel returns the input channel of Operation WorkerPool
func (fwp *OperationWorkerPool) GetInputChannel() (chan any, error) {
	return fwp.inputChannel, nil
}

// GetOutputChannel returns the output channel of Operation WorkerPool
func (fwp *OperationWorkerPool) GetOutputChannel() (chan any, error) {
	return fwp.outputChannel, nil
}

// SetInputChannel updates the input channel of Operation WorkerPool
func (fwp *OperationWorkerPool) SetInputChannel(inChan chan any) error {
	fwp.inputChannel = inChan
	return nil
}

// SetOutputChannel updates the output channel of Operation WorkerPool
func (fwp *OperationWorkerPool) SetOutputChannel(outChan chan any) error {
	fwp.outputChannel = outChan
	return nil
}

// Start Operation Worker Pool
func (fwp *OperationWorkerPool) Start(ctx CnvContext) error {
	switch fwp.Mode {
	case WorkerModeTransaction:
		return fwp.startTransactionMode(ctx)
	case WorkerModeLoop:
		return fwp.startLoopMode(ctx)
	default:
		return ErrInvalidWorkerMode
	}
}

// startLoopMode OperationWorkerPool
func (fwp *OperationWorkerPool) startLoopMode(ctx CnvContext) error {

	return fwp.ConcreteNodeWorker.startLoopMode(ctx, fwp.inputChannel, fwp.outputChannel)

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

		inData, ok := <-fwp.inputChannel
		if !ok {
			ctx.SendLog(0, fmt.Sprintf("Executor:[%s] Operation's input channel closed", fwp.Executor.GetUniqueIdentifier()), nil)
			break workerLoop
		}

		if err := fwp.sem.Acquire(ctx, 1); err != nil {
			ctx.SendLog(0, fmt.Sprintf("Executor:[%s], sem acquire failed", fwp.Executor.GetUniqueIdentifier()), err)
			break workerLoop
		}

		go func(data any) {
			defer fwp.recovery(ctx, "OperationWorkerPool")
			defer fwp.sem.Release(1)

			out, err := fwp.Executor.executeUntyped(ctx, data)
			switch err {
			case nil:
				select {
				case <-ctx.Done():
					return
				default:
				}
				fwp.outputChannel <- out
			case ErrExecuteNotImplemented:
				ctx.SendLog(0, fmt.Sprintf("Executor:[%s]", fwp.Executor.GetUniqueIdentifier()), err)
				log.Fatalf("Improper setup of Executor[%s], Execute() method is required", fwp.Executor.GetUniqueIdentifier())
			default:
				ctx.SendLog(2, fmt.Sprintf("Worker:[%s] for Executor:[%s] Execute() Call Failed.",
					fwp.Name, fwp.Executor.GetUniqueIdentifier()), err)
			}
		}(inData)

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
