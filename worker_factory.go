package conveyor

var nodeWorkers = map[string]func(executor nodeExecutor, mode WorkerMode) NodeWorker{

	WorkerTypeSource:    NewSourceWorkerPool,
	WorkerTypeOperation: NewOperationWorkerPool,
	WorkerTypeSink:      NewSinkWorkerPool,
}

func newNodeWorker(executor nodeExecutor, mode WorkerMode, workerType string) (NodeWorker, error) {

	if _, ok := nodeWorkers[workerType]; ok {
		return nodeWorkers[workerType](executor, mode), nil
	}

	return nil, ErrInvalidWorkerType
}
