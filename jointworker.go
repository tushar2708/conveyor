package conveyor

import "log"

// // FetchHandler interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
// type FetchHandler interface {
// 	FetchAndSend(inputChannel chan interface{}, outputChannel []chan interface{})
// 	Start()
// 	Stop()
// 	GetPool() *FetchWorkerPool
// }

// JointWorkerPool struct provides the worker pool infra for Fetch interface
type JointWorkerPool struct {
	ConcreteJointWorker
	nextWorkerCount int
	inputChannels   []chan map[string]interface{}
	outputChannels  []chan map[string]interface{}
}

// NewJointWorkerPool creates a new FetchWorkerPool
func NewJointWorkerPool(name string, handler JointHandler, buffer int) JointWorker {
	jwp := &JointWorkerPool{
		ConcreteJointWorker: ConcreteJointWorker{
			WPool: WPool{
				Name: name,
			},
			Handler: handler,
		},
	}

	for i := 0; i < jwp.Handler.InputCount(); i++ {
		jwp.inputChannels = append(jwp.inputChannels, make(chan map[string]interface{}, buffer))
	}

	return jwp
}

// GetInputChannels returns the input channel of Fetch WorkerPool
func (jwp *JointWorkerPool) GetInputChannels() ([]chan map[string]interface{}, error) {
	return jwp.inputChannels, nil
}

// GetOutputChannels returns the output channel of Fetch WorkerPool
func (jwp *JointWorkerPool) GetOutputChannels() ([]chan map[string]interface{}, error) {
	return jwp.outputChannels, nil
}

// SetInputChannels updates the input channel of Source WorkerPool
func (jwp *JointWorkerPool) SetInputChannels(inChans []chan map[string]interface{}) error {
	jwp.inputChannels = inChans
	return nil
}

// SetOutputChannels updates the output channel of Source WorkerPool
func (jwp *JointWorkerPool) SetOutputChannels(outChans []chan map[string]interface{}) error {
	jwp.outputChannels = outChans
	return nil
}

// AddInputChannel maps a slice of channels on the join't outupt channels
func (jwp *JointWorkerPool) AddInputChannel(inChan chan map[string]interface{}) error {
	jwp.outputChannels = append(jwp.outputChannels, inChan)
	return nil
}

// AddOutputChannel maps a slice of channels on the join't outupt channels
func (jwp *JointWorkerPool) AddOutputChannel(outChan chan map[string]interface{}) error {
	jwp.outputChannels = append(jwp.outputChannels, outChan)
	return nil
}

// Start JoinWorkerPool
func (jwp *JointWorkerPool) Start() error {
	for i := 0; i < jwp.Handler.Count(); i++ {
		jwp.Wg.Add(1)
		go func() {
			defer jwp.Wg.Done()
			if err := jwp.Handler.Execute(jwp.inputChannels, jwp.outputChannels); err != nil {
				log.Println("join handler start failed", err)
				return
			}
		}()
	}
	return nil
}

// Stop FetchWorkerPool
func (jwp *JointWorkerPool) Stop() error {
	jwp.Wg.Wait()

	for _, ch := range jwp.outputChannels {
		close(ch)
	}
	return nil
}
