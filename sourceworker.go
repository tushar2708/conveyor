package conveyor

// SourceWorkerPool struct provides the worker pool infra for Source interface
type SourceWorkerPool struct {
	ConcreteNodeWorker
	nextWorkerCount int
	outputChannel   chan map[string]interface{}
}

// NewSourceWorkerPool creates a new SourceWorkerPool
func NewSourceWorkerPool(name string, handler NodeHandler, buffer int) NodeWorker {

	swp := &SourceWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: name,
			},
			Handler: handler,
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

// Start SourceWorkerPool
func (swp *SourceWorkerPool) Start() error {
	for i := 0; i < swp.Handler.Count(); i++ {
		swp.Wg.Add(1)
		go func() {
			defer swp.Wg.Done()
			swp.Handler.Execute(nil, swp.outputChannel)
		}()
	}
	return nil
}

// WorkerType returns the type of worker
func (swp *SourceWorkerPool) WorkerType() string {
	return SourceWorkerType
}

// Stop SourceWorkerPool
func (swp *SourceWorkerPool) Stop() error {
	swp.Wg.Wait()
	close(swp.outputChannel)
	return nil
}
