package conveyor

// SinkWorkerPool struct provides the worker pool infra for Sink interface
type SinkWorkerPool struct {
	ConcreteNodeWorker
	inputChannel chan map[string]interface{}
}

// NewSinkWorkerPool creates a new SinkWorkerPool
func NewSinkWorkerPool(executor NodeExecutor) NodeWorker {

	swp := &SinkWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: executor.GetName() + "_worker",
			},
			Executor: executor,
		},
	}

	return swp
}

// CreateChannels creates channels for the sink worker
func (swp *SinkWorkerPool) CreateChannels(buffer int) {
	swp.inputChannel = make(chan map[string]interface{}, buffer)
}

// StartLoopMode SinkWorkerPool
func (swp *SinkWorkerPool) StartLoopMode(ctx *CnvContext) error {
	for i := 0; i < swp.Executor.Count(); i++ {
		swp.Wg.Add(1)

		go func() {
			defer swp.Wg.Done()
			swp.Executor.ExecuteLoop(ctx, swp.inputChannel, nil)
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
func (swp *SinkWorkerPool) WaitAndStop() error {
	swp.Wg.Wait()
	swp.Executor.CleanUp()
	return nil
}
