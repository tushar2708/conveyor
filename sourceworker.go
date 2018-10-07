package conveyor

// SourceWorkerPool struct provides the worker pool infra for Source interface
type SourceWorkerPool struct {
	ConcreteNodeWorker
	nextWorkerCount int
	outputChannel   chan map[string]interface{}
}

// NewSourceWorkerPool creates a new SourceWorkerPool
func NewSourceWorkerPool(executor NodeExecutor) NodeWorker {

	swp := &SourceWorkerPool{
		ConcreteNodeWorker: ConcreteNodeWorker{
			WPool: WPool{
				Name: executor.GetName() + "_worker",
			},
			Executor: executor,
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

// StartLoopMode SourceWorkerPool
func (swp *SourceWorkerPool) StartLoopMode(ctx *CnvContext) error {
	for i := 0; i < swp.Executor.Count(); i++ {
		swp.Wg.Add(1)
		go func() {
			defer swp.Wg.Done()
			swp.Executor.ExecuteLoop(ctx, nil, swp.outputChannel)
		}()
	}
	return nil
}

// WorkerType returns the type of worker
func (swp *SourceWorkerPool) WorkerType() string {
	return SourceWorkerType
}

// WaitAndStop SourceWorkerPool
func (swp *SourceWorkerPool) WaitAndStop() error {
	swp.Wg.Wait()
	swp.Executor.CleanUp()
	close(swp.outputChannel)
	return nil
}
