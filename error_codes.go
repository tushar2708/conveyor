package conveyor

import "errors"

var (
	// ErrInvalidWorkerType error
	ErrInvalidWorkerType = errors.New("Invalid worker type. pick one from conveyor.WorkerTypeSource/conveyor.WorkerTypeOperation/conveyor.WorkerTypeSink")

	// ErrInvalidWorkerMode error
	ErrInvalidWorkerMode = errors.New("Invalid worker mode. pick either conveyor.WorkerModeTransaction or conveyor.WorkerModeLoop")

	// ErrNoNodesAvailable error
	ErrNoNodesAvailable = errors.New("Your action assumes presence of Node Executors in conveyor, but none were found")

	// ErrNoJointsAvailable error
	ErrNoJointsAvailable = errors.New("Your action assumes presence of Joint Executors in conveyor, but none were found")

	// ErrExecuteNotImplemented error
	ErrExecuteNotImplemented = errors.New("This executor doesn't implement Execute() method")

	// ErrExecuteLoopNotImplemented error
	ErrExecuteLoopNotImplemented = errors.New("This executor doesn't implement ExecuteLoop() method")

	// ErrInputChanDoesNotExist error
	ErrInputChanDoesNotExist = errors.New("input channel doesn't exist for this node")
	// ErrOutputChanDoesNotExist error
	ErrOutputChanDoesNotExist = errors.New("output channel doesn't exist for this node")

	// ErrSourceExhausted error
	ErrSourceExhausted = errors.New("Source executor is exhausted")
	// ErrSourceInternal error
	ErrSourceInternal = errors.New("Source executor internal error")

	// ErrFetchRejected error
	ErrFetchRejected = errors.New("Fetch executor rejected the transaction")
	// ErrFetchInternal error
	ErrFetchInternal = errors.New("Fetch executor internal error")

	// ErrSinkRejected error
	ErrSinkRejected = errors.New("Sink executor rejected data")
	// ErrSinkInternal error
	ErrSinkInternal = errors.New("Sink executor internal error")

	// ErrLessInputChannelsInJoint error
	ErrLessInputChannelsInJoint = errors.New("JointWorker doesn't have enough input channels")

	// ErrLessOutputChannelsInJoint error
	ErrLessOutputChannelsInJoint = errors.New("JointWorker doesn't have enough output channels")

	// ErrLifeCycleNotSupported error
	ErrLifeCycleNotSupported = errors.New("This conveyor instance isn't created with Life Cycle Support")
)
