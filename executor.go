package conveyor

import (
	"fmt"
)

// NodeExecutor interface is the interface that you need to implement in your own types of nodes
type NodeExecutor interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error
	Execute(ctx CnvContext, inData map[string]interface{}) (map[string]interface{}, error)
	Count() int
	CleanUp() error
}

// JointExecutor interface is the interface that you need to implement in your own types of joints
type JointExecutor interface {
	GetName() string
	GetUniqueIdentifier() string
	ExecuteLoop(ctx CnvContext, inChan []chan map[string]interface{}, outChan []chan map[string]interface{}) error
	Count() int
	InputCount() int
	OutputCount() int
}

// ConcreteNodeExecutor struct represents a concrete node structure, you should compose it into your node structures
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

// Execute should take a "map[string]interface{}" as input and returns a map[string]interface{}" as output
// Ideally it should process the input and either return a new map, or just add it's own keys to it,
// if we want to retain the data from previous node.
// This base implementation, just returns an error, so you need to override it with your own.
// Any struct may define both Execute & ExecuteLoop.Execute
// Execute will be used if mode is set to conveyor.WorkerModeTransaction
func (cnh *ConcreteNodeExecutor) Execute(ctx CnvContext, inData map[string]interface{}) (map[string]interface{}, error) {
	return nil, ErrExecuteNotImplemented
}

// ExecuteLoop should take two "map[string]interface{}" channels.
// It is a more hands-on version of Execute() method,
// where you have to handle reading from input channel and writing to output channel, after processing on your own
// This base implementation, just returns an error, so you need to override it with your own.
// Any struct may define both Execute & ExecuteLoop.
// ExecuteLoop will be used if mode is set to conveyor.WorkerModeLoop
func (cnh *ConcreteNodeExecutor) ExecuteLoop(ctx CnvContext, inChan <-chan map[string]interface{}, outChan chan<- map[string]interface{}) error {
	return ErrExecuteNotImplemented
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
