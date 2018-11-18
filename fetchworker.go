package conveyor

import (
	"fmt"
	"log"

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

	wCnt := executor.Count()
	if wCnt < 1 {
		wCnt = 1
	}

	fwp := &FetchWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: executor.GetName() + "_worker",
			},
			WorkerCount: wCnt,
			Mode:        mode,
			Executor:    executor,
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

	for i := 0; i < fwp.WorkerCount; i++ {
		fwp.Wg.Add(1)
		go func() {
			defer fwp.Wg.Done()

			if err := fwp.Executor.ExecuteLoop(ctx, fwp.inputChannel, fwp.outputChannel); err != nil {
				if err == ErrExecuteLoopNotImplemented {
					ctx.SendLog(3, fmt.Sprintf("Executor:[%s] ", fwp.Executor.GetUniqueIdentifier()), err)

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
			ctx.SendLog(3, fmt.Sprintf("Executor:[%s] fetch's input channel closed", fwp.Executor.GetUniqueIdentifier()), nil)
			break workerLoop
		}

		if err := fwp.sem.Acquire(ctx, 1); err != nil {
			ctx.SendLog(3, fmt.Sprintf("Executor:[%s], sem acquire failed", fwp.Executor.GetUniqueIdentifier()), err)
			break workerLoop
		}
		// fmt.Println("fetch sem acquire 1")

		go func(data map[string]interface{}) {
			// defer fmt.Println("fetch sem release 1")
			defer fwp.sem.Release(1)

			out, err := fwp.Executor.Execute(ctx, in)
			if err == nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				fwp.outputChannel <- out
			} else if err == ErrExecuteNotImplemented {
				ctx.SendLog(3, fmt.Sprintf("Executor:[%s]", fwp.Executor.GetUniqueIdentifier()), err)
				log.Fatalf("Improper setup of Executor[%s], Execute() method is required", fwp.Executor.GetUniqueIdentifier())
			}
			return
		}(in)

	}

	return nil
}

// WorkerType returns the type of worker
func (fwp *FetchWorkerPool) WorkerType() string {
	return FetchWorkerType
}

// WaitAndStop FetchWorkerPool
func (fwp *FetchWorkerPool) WaitAndStop(ctx *CnvContext) error {

	select {
	case <-ctx.Done():
		return nil
	default:
	}

	if fwp.Mode == WorkerModeTransaction {
		if err := fwp.sem.Acquire(ctx, int64(fwp.WorkerCount)); err != nil {
			ctx.SendLog(3, fmt.Sprintf("Worker:[%s] for Executor:[%s] Failed to acquire semaphore", fwp.Name, fwp.Executor.GetUniqueIdentifier()), err)
		}
	} else {
		fwp.Wg.Wait()
	}

	fwp.Executor.CleanUp()
	close(fwp.outputChannel)
	return nil
}
