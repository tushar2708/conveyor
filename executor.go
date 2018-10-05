package conveyor

// NodeExecutor interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type NodeExecutor interface {
	Execute(ctx *CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{})
	Count() int
	CleanUp() error
}

// JointExecutor interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type JointExecutor interface {
	Execute(ctx *CnvContext, inChan []chan map[string]interface{}, outChan []chan map[string]interface{}) error
	Count() int
	InputCount() int
	OutputCount() int
}

// ConcreteNodeExecutor struct represents a concrete node structure
type ConcreteNodeExecutor struct {
	Name string
	Data interface{}
}

// Count returns the number of executors required
func (cnh *ConcreteNodeExecutor) Count() int {
	return 1
}

// CleanUp does any cleanup if needed after executors are done
func (cnh *ConcreteNodeExecutor) CleanUp() error {
	return nil
}

// ConcreteJointExecutor struct represents a concrete node structure
type ConcreteJointExecutor struct {
	Name string
}

// Count returns the number of executors required for joint
func (cjh *ConcreteJointExecutor) Count() int {
	return 1
}

// InputCount returns the number of executors required
func (cjh *ConcreteJointExecutor) InputCount() int {
	return 1
}

// OutputCount returns the number of executors required
func (cjh *ConcreteJointExecutor) OutputCount() int {
	return 1
}
