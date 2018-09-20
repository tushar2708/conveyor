package conveyor

// NodeHandler interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type NodeHandler interface {
	Execute(inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{})
	Count() int
	CleanUp() error
}

// JointHandler interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type JointHandler interface {
	Execute(inChan []chan map[string]interface{}, outChan []chan map[string]interface{}) error
	Count() int
	InputCount() int
	OutputCount() int
}

// ConcreteNodeHandler struct represents a concrete node structure
type ConcreteNodeHandler struct {
	Name string
	Data interface{}
}

// Count returns the number of executors required
func (cnh *ConcreteNodeHandler) Count() int {
	return 1
}

// CleanUp does any cleanup if needed after executors are done
func (cnh *ConcreteNodeHandler) CleanUp() error {
	return nil
}

// ConcreteJointHandler struct represents a concrete node structure
type ConcreteJointHandler struct {
	Name string
}

// Count returns the number of executors required for joint
func (cjh *ConcreteJointHandler) Count() int {
	return 1
}

// InputCount returns the number of executors required
func (cjh *ConcreteJointHandler) InputCount() int {
	return 1
}

// OutputCount returns the number of executors required
func (cjh *ConcreteJointHandler) OutputCount() int {
	return 1
}
