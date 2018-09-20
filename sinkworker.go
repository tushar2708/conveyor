package conveyor

// SinkWorkerPool struct provides the worker pool infra for Sink interface
type SinkWorkerPool struct {
	ConcreteNodeWorker
	inputChannel chan map[string]interface{}
}

// NewSinkWorkerPool creates a new SinkWorkerPool
func NewSinkWorkerPool(name string, handler NodeHandler, buffer int) NodeWorker {

	swp := &SinkWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: name,
			},
			Handler: handler,
		},
	}

	swp.inputChannel = make(chan map[string]interface{}, buffer)

	return swp
}

// Start SinkWorkerPool
func (swp *SinkWorkerPool) Start() error {
	for i := 0; i < swp.Handler.Count(); i++ {
		swp.Wg.Add(1)

		go func() {
			defer swp.Wg.Done()
			swp.Handler.Execute(swp.inputChannel, nil)
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

// Stop SinkWorkerPool
func (swp *SinkWorkerPool) Stop() error {
	swp.Wg.Wait()
	return nil
}
