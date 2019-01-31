package conveyor

import (
	"errors"
)

// ReplicateJoint is a plumbing joint that connects a source/operation node to multiple operation/sink nodes
type ReplicateJoint struct {
	*ConcreteJointExecutor
	OutChanCount int
}

var (
	// ErrNoInputChannel error
	ErrNoInputChannel = errors.New("number of input channels is 0")

	// ErrNoOutputChannel error
	ErrNoOutputChannel = errors.New("number of output channels is 0")

	// ErrMultipleInputChannels error
	ErrMultipleInputChannels = errors.New("only one input channel can be replicated")

	// ErrOneToOneConnection error
	ErrOneToOneConnection = errors.New("replicate joint isn't needed for one-to one mapping, " +
		"you can just link the nodes directly")
)

// NewReplicateJoint creates a new joint to replicate same data to multiple channels
func NewReplicateJoint(name string, outChanCount int) (*ReplicateJoint, error) {

	rj := &ReplicateJoint{
		ConcreteJointExecutor: &ConcreteJointExecutor{
			Name: name,
		},
		OutChanCount: outChanCount,
	}
	return rj, nil
}

// NewReplicateJointWithContext creates a new joint (with context) to replicate same data to multiple channels
func NewReplicateJointWithContext(cnvCtx CnvContext, name string, outChanCount int) (*ReplicateJoint, error) {

	rj := &ReplicateJoint{
		ConcreteJointExecutor: &ConcreteJointExecutor{
			Name: name,
		},
		OutChanCount: outChanCount,
	}
	return rj, nil
}

// ExecuteLoop method produces data for other nodes from inputChannel file, and broadcasts copies of this data on all of it's output channels
func (rj *ReplicateJoint) ExecuteLoop(cnvCtx CnvContext, inChans []chan map[string]interface{}, outChans []chan map[string]interface{}) error {

	// log.Printf("going to replicate data to %d channels", rj.OutChanCount)

	inChanCount := len(inChans)
	outChanCount := len(outChans)

	if inChanCount == 0 {
		return ErrNoInputChannel
	}

	if outChanCount == 0 {
		return ErrNoOutputChannel
	}

	if inChanCount > 1 {
		return ErrMultipleInputChannels
	}

	if inChanCount == 1 && outChanCount == 1 {
		return ErrMultipleInputChannels
	}

	inChan := inChans[0]

	for input := range inChan {
		for _, outCh := range outChans {
			outCh <- input
		}
	}

	return nil
}

// OutputCount returns the number of executors required
func (rj *ReplicateJoint) OutputCount() int {
	return rj.OutChanCount
}
