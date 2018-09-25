package conveyor

// // FetchHandler interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
// type FetchHandler interface {
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
func NewFetchWorkerPool(name string, handler NodeHandler, buffer int) NodeWorker {

	fwp := &FetchWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: name,
			},
			Handler: handler,
		},
	}

	fwp.inputChannel = make(chan map[string]interface{}, buffer)
	return fwp
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

// Start FetchWorkerPool
func (fwp *FetchWorkerPool) Start(ctx *CnvContext) error {
	for i := 0; i < fwp.Handler.Count(); i++ {
		fwp.Wg.Add(1)
		go func() {
			defer fwp.Wg.Done()
			fwp.Handler.Execute(ctx, fwp.inputChannel, fwp.outputChannel)
		}()
	}
	return nil
}

// WorkerType returns the type of worker
func (fwp *FetchWorkerPool) WorkerType() string {
	return FetchWorkerType
}

// WaitAndStop FetchWorkerPool
func (fwp *FetchWorkerPool) WaitAndStop() error {
	fwp.Wg.Wait()
	close(fwp.outputChannel)
	return nil
}
