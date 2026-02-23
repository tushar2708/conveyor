package conveyor

// ReplicateJoint is a generic plumbing joint that connects a source/operation node
// to multiple operation/sink nodes. It replicates the same data to all output channels.
type ReplicateJoint[T any] struct {
	Name         string
	OutChanCount int
}

// NewReplicateJoint creates a new generic joint to replicate same data to multiple channels.
func NewReplicateJoint[T any](name string, outChanCount int) (*ReplicateJoint[T], error) {
	rj := &ReplicateJoint[T]{
		Name:         name,
		OutChanCount: outChanCount,
	}
	return rj, nil
}

// GetName returns the name of the joint executor.
func (rj *ReplicateJoint[T]) GetName() string {
	return rj.Name
}

// GetUniqueIdentifier returns a unique string identifying this executor.
func (rj *ReplicateJoint[T]) GetUniqueIdentifier() string {
	return rj.Name
}

// Count returns the number of concurrent executor instances required.
func (rj *ReplicateJoint[T]) Count() int {
	return 1
}

// InputCount returns the number of input channels this joint accepts.
func (rj *ReplicateJoint[T]) InputCount() int {
	return 1
}

// OutputCount returns the number of output channels this joint fans out to.
func (rj *ReplicateJoint[T]) OutputCount() int {
	return rj.OutChanCount
}

// ExecuteLoop reads from a single input channel and broadcasts each value to all output channels.
// It returns an error immediately if the channel configuration is invalid; otherwise it
// runs until the input channel is closed, then returns nil.
func (rj *ReplicateJoint[T]) ExecuteLoop(cnvCtx CnvContext, inChans []chan T, outChans []chan T) error {
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
		return ErrOneToOneConnection
	}

	inChan := inChans[0]

	for input := range inChan {
		for _, outCh := range outChans {
			outCh <- input
		}
	}

	return nil
}
