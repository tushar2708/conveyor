package conveyor

import "errors"

var (
	// ErrInvalidWorkerType error
	ErrInvalidWorkerType = errors.New("invalid worker type: pick one from conveyor.WorkerTypeSource/conveyor.WorkerTypeOperation/conveyor.WorkerTypeSink")

	// ErrInvalidWorkerMode error
	ErrInvalidWorkerMode = errors.New("invalid worker mode: pick either conveyor.WorkerModeTransaction or conveyor.WorkerModeLoop")

	// ErrNoNodesAvailable error
	ErrNoNodesAvailable = errors.New("action assumes presence of node executors in conveyor, but none were found")

	// ErrNoJointsAvailable error
	ErrNoJointsAvailable = errors.New("action assumes presence of joint executors in conveyor, but none were found")

	// ErrExecuteNotImplemented error
	ErrExecuteNotImplemented = errors.New("executor does not implement Execute() method")

	// ErrExecuteLoopNotImplemented error
	ErrExecuteLoopNotImplemented = errors.New("executor does not implement ExecuteLoop() method")

	// ErrInputChanDoesNotExist error
	ErrInputChanDoesNotExist = errors.New("input channel doesn't exist for this node")
	// ErrOutputChanDoesNotExist error
	ErrOutputChanDoesNotExist = errors.New("output channel doesn't exist for this node")

	// ErrSourceExhausted error
	ErrSourceExhausted = errors.New("source executor is exhausted")
	// ErrSourceInternal error
	ErrSourceInternal = errors.New("source executor internal error")

	// ErrFetchRejected error
	ErrFetchRejected = errors.New("fetch executor rejected the transaction")
	// ErrFetchInternal error
	ErrFetchInternal = errors.New("fetch executor internal error")

	// ErrSinkRejected error
	ErrSinkRejected = errors.New("sink executor rejected data")
	// ErrSinkInternal error
	ErrSinkInternal = errors.New("sink executor internal error")

	// ErrLessInputChannelsInJoint error
	ErrLessInputChannelsInJoint = errors.New("joint worker doesn't have enough input channels")

	// ErrLessOutputChannelsInJoint error
	ErrLessOutputChannelsInJoint = errors.New("joint worker doesn't have enough output channels")

	// ErrLifeCycleNotSupported error
	ErrLifeCycleNotSupported = errors.New("conveyor instance is not created with life cycle support")

	// ErrTypeMismatch is returned when adjacent nodes have incompatible types
	ErrTypeMismatch = errors.New("type mismatch between adjacent pipeline nodes")

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
