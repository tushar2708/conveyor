package conveyor

import "errors"

var (

	// ErrExecuteNotImplemented error
	ErrExecuteNotImplemented = errors.New("This executor doesn't implement Execute() method")

	// ErrInvalidWorkerMode error
	ErrInvalidWorkerMode = errors.New("Invalid worker mode. pick either conveyor.WorkerModeTransaction or conveyor.WorkerModeLoop")

	// ErrInputChanDoesNotExist error
	ErrInputChanDoesNotExist = errors.New("input channel doesn't exist for this node")
	// ErrOutputChanDoesNotExist error
	ErrOutputChanDoesNotExist = errors.New("output channel doesn't exist for this node")

	// ErrExecuteLoopNotImplemented error
	ErrExecuteLoopNotImplemented = errors.New("This executor doesn't implement ExecuteLoop() method")

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
