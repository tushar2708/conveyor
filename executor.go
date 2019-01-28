package conveyor

import (
	"fmt"
)

// NodeExecutor interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type NodeExecutor interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error
	Execute(ctx CnvContext, inData map[string]interface{}) (map[string]interface{}, error)
	Count() int
	CleanUp() error
}

// JointExecutor interface binds to nodes that have the capability to fetch intermidiate data, and forward it to next node
type JointExecutor interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChan []chan map[string]interface{}, outChan []chan map[string]interface{}) error
	Count() int
	InputCount() int
	OutputCount() int
}

// ConcreteNodeExecutor struct represents a concrete node structure
type ConcreteNodeExecutor struct {
	Name string
	Data interface{}
}

// GetUniqueIdentifier can be used to fetch a unique string identifying the executor
func (cnh *ConcreteNodeExecutor) GetUniqueIdentifier() string {
	return fmt.Sprintf("%s", cnh.Name)
}

// Count returns the number of executors required
func (cnh *ConcreteNodeExecutor) Count() int {
	return 1
}

// GetName returns the name of the executor
func (cnh *ConcreteNodeExecutor) GetName() string {
	return cnh.Name
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

// GetName returns the name of the executor
func (cjh *ConcreteJointExecutor) GetName() string {
	return cjh.Name
}

// GetUniqueIdentifier can be used to fetch a unique string identifying the executor
func (cjh *ConcreteJointExecutor) GetUniqueIdentifier() string {
	return fmt.Sprintf("%s", cjh.Name)
}

// InputCount returns the number of executors required
func (cjh *ConcreteJointExecutor) InputCount() int {
	return 1
}

// OutputCount returns the number of executors required
func (cjh *ConcreteJointExecutor) OutputCount() int {
	return 1
}
