package conveyor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Conveyor is base
type Conveyor struct {
	Name         string
	ctx          CnvContext
	needProgress bool
	tickProgress time.Duration
	bufferLen    int

	progress         chan float64
	duration         time.Duration
	expectedDuration time.Duration
	lcHandler        LifeCycleHandler

	workers []NodeWorker
	joints  []JointWorker

	cleanupOnce sync.Once

	// SourceNode NodeWorker
	// InnerNodes []NodeWorker
	// SinkNodes  []NodeWorker
}

// Status returns a channel on which Conveyor Statuses will be published
func (cnv *Conveyor) Status() <-chan string {
	iCtxData := cnv.ctx.GetData()
	if iCtxData != nil {
		ctxData, _ := iCtxData.(CtxData)
		return ctxData.status
	}
	return nil
}

// GetConveyorContext gives the conveyor's context object
func (cnv *Conveyor) GetConveyorContext() CnvContext {
	return cnv.ctx
}

// Done returns the context.Done() channel of Conveyor
func (cnv *Conveyor) Done() <-chan struct{} {
	return cnv.ctx.Done()
}

// Progress returns a channel which is regularly updated with progress %
func (cnv *Conveyor) Progress() <-chan float64 {
	if cnv.needProgress {
		return cnv.progress
	}
	return nil
}

// Logs returns a channel on which Conveyor Statuses will be published
func (cnv *Conveyor) Logs() <-chan Message {
	iCtxData := cnv.ctx.GetData()
	if iCtxData != nil {
		ctxData, _ := iCtxData.(CtxData)
		return ctxData.logs
	}
	return nil
}

var (
	// ErrEmptyConveyor error
	ErrEmptyConveyor = errors.New("conveyor is empty, no workers employed")
)

// NewConveyor creates a new Conveyor instance
func NewConveyor(name string, bufferLen int) (*Conveyor, error) {

	conveyor, err := newConveyorWithInbuiltCtx(name, bufferLen, nil, -1)
	conveyor.needProgress = false

	return conveyor, err
}

// NewTimeoutConveyor creates a new Conveyor instance with specified timeout
func NewTimeoutConveyor(name string, bufferLen int, timeout time.Duration) (*Conveyor, error) {

	conveyor, err := newConveyorWithInbuiltCtx(name, bufferLen, nil, timeout)
	conveyor.needProgress = false

	return conveyor, err
}

// NewTimeoutAndProgressConveyor creates a new Conveyor instance
func NewTimeoutAndProgressConveyor(name string, bufferLen int, lch LifeCycleHandler, timeout, expectedDuration time.Duration) (*Conveyor, error) {

	if expectedDuration == 0 {
		expectedDuration = time.Hour
	}

	conveyor, err := newConveyorWithInbuiltCtx(name, bufferLen, lch, timeout)

	conveyor.needProgress = true
	conveyor.progress = make(chan float64, 1)

	conveyor.expectedDuration = expectedDuration
	conveyor.tickProgress = time.Millisecond * 500

	return conveyor, err
}

// NewConveyorWithCustomCtx creates a new Conveyor instance with custom context management
func NewConveyorWithCustomCtx(name string, bufferLen int, lch LifeCycleHandler, timeout, expectedDuration time.Duration, _ctx CnvContext) (*Conveyor, error) {

	if expectedDuration == 0 {
		expectedDuration = time.Hour
	}

	conveyor, err := newConveyor(name, bufferLen, lch, timeout, _ctx)

	conveyor.needProgress = true
	conveyor.progress = make(chan float64, 1)

	conveyor.expectedDuration = expectedDuration
	conveyor.tickProgress = time.Millisecond * 500

	return conveyor, err
}

func newConveyorWithInbuiltCtx(name string, bufferLen int, lch LifeCycleHandler, timeout time.Duration) (*Conveyor, error) {

	if bufferLen <= 0 {
		bufferLen = 100
	}

	_ctx := &cnvContext{
		Context: context.Background(),
		Data: CtxData{
			Name:   name,
			logs:   make(chan Message, 100),
			status: make(chan string, 100),
		},
	}
	var ctx CnvContext
	if timeout <= 0 {
		ctx = _ctx.WithCancel()
	} else {
		ctx = _ctx.WithTimeout(timeout)
	}

	cnv := &Conveyor{
		Name:      name,
		bufferLen: bufferLen,
		ctx:       ctx,
		lcHandler: lch,
	}

	return cnv, nil
}

func newConveyor(name string, bufferLen int, lch LifeCycleHandler, timeout time.Duration, _ctx CnvContext) (*Conveyor, error) {

	if bufferLen <= 0 {
		bufferLen = 100
	}

	var ctx CnvContext
	if timeout <= 0 {
		ctx = _ctx.WithCancel()
	} else {
		ctx = _ctx.WithTimeout(timeout)
	}

	cnv := &Conveyor{
		Name:      name,
		bufferLen: bufferLen,
		ctx:       ctx,
		lcHandler: lch,
	}

	return cnv, nil
}

// GetLastWorker returns the last added worker, or error if conveyor is empty
func (cnv *Conveyor) GetLastWorker() (NodeWorker, error) {
	workerCount := len(cnv.workers)
	if workerCount > 0 {
		return cnv.workers[workerCount-1], nil
	}

	return nil, ErrEmptyConveyor
}

// AddNodeExecutor creates a worker for a given executor (based on workerMode & workerType)
// And then links it to the last "Node" added to the conveyor, by creating and mapping connecting channels
// In case there was no node added previously, it skips the linking part
func (cnv *Conveyor) AddNodeExecutor(nodeExecutor NodeExecutor, workerMode WorkerMode, workerType string) error {
	nodeWorker, err := newNodeWorker(nodeExecutor, workerMode, workerType)
	if err != nil {
		return err
	}

	if addErr := cnv.AddNodeWorker(nodeWorker, true); addErr != nil {
		fmt.Printf("Adding %s [type:%s] to conveyor failed. Error:[%v]\n",
			nodeExecutor.GetName(), workerType, addErr)
		return addErr
	}
	return nil
}

// AddJointExecutor creates a worker for a given executor (based on workerMode & workerType)
// And then links it to the last "Node" added to the conveyor, by creating and mapping connecting channels
// In case there was no node added previously, it skips the linking part
func (cnv *Conveyor) AddJointExecutor(jointExecutor JointExecutor) error {

	jointWorker := NewJointWorkerPool(jointExecutor)

	if err := cnv.AddJointWorker(jointWorker); err != nil {
		fmt.Printf("Adding %s to conveyor failed. Error:[%v]\n",
			jointExecutor.GetName(), err)
		return err
	}
	return nil
}

// AddNodeExecutorToJoint creates a worker for a given executor (based on workerMode & workerType)
// And then links it to the last "Joint" added to the conveyor, by creating and mapping connecting channels
// In case there was no "Joint" added previously, it returns an error
func (cnv *Conveyor) AddJointExecutorAfterNode(jointExecutor JointExecutor, workerMode WorkerMode, workerType string) error {

	nodeCount := len(cnv.workers)

	if nodeCount == 0 {
		return ErrNoNodesAvailable
	}

	jointWorker := NewJointWorkerPool(jointExecutor)

	if addErr := cnv.AddJointWorker(jointWorker); addErr != nil {
		fmt.Printf("Adding joint-%s after node[type:%s] to conveyor failed. Error:[%v]\n",
			jointExecutor.GetName(), workerType, addErr)
		return addErr
	}

	// Pick the last node that was added to this conveyor
	nodeWorker := cnv.workers[nodeCount-1]

	// Link last node before sinks to 0th input channel of the broadcast joint
	linkErr := LinkJointAfterNode(nodeWorker, jointWorker, 0)
	if linkErr != nil {
		return linkErr
	}

	return nil
}

// AddNodeExecutorToJoint creates a worker for a given executor (based on workerMode & workerType)
// And then links it to the last "Joint" added to the conveyor, by creating and mapping connecting channels
// In case there was no "Joint" added previously, it returns an error
func (cnv *Conveyor) AddNodeExecutorAfterJoint(nodeExecutor NodeExecutor, workerMode WorkerMode, workerType string) error {

	jointCount := len(cnv.joints)

	if jointCount == 0 {
		return ErrNoJointsAvailable
	}

	nodeWorker, err := newNodeWorker(nodeExecutor, workerMode, workerType)
	if err != nil {
		return err
	}

	// Add nodeWorker to list, but don't link it's channels yet
	if addErr := cnv.AddNodeWorker(nodeWorker, false); addErr != nil {
		fmt.Printf("Adding node-%s[type:%s] after joint to conveyor failed. Error:[%v]\n",
			nodeExecutor.GetName(), workerType, addErr)
		return addErr
	}

	// Pick the last joint that was added to this conveyor
	jointWorker := cnv.joints[jointCount-1]

	// Now link the newly created node after last joint
	if err := LinkNodeAfterJoint(jointWorker, nodeWorker); err != nil {
		fmt.Printf("Adding node-%s[type:%s] after joint to conveyor failed. Error:[%v]\n",
			nodeExecutor.GetName(), workerType, err)
		return err
	}

	return nil
}

// AddNodeWorker employs a new worker station to the conveyor
func (cnv *Conveyor) AddNodeWorker(worker NodeWorker, toLink bool) error {
	worker.CreateChannels(cnv.bufferLen)
	cnv.workers = append(cnv.workers, worker)

	workerCount := len(cnv.workers)
	if toLink && workerCount > 1 {
		currWorker := cnv.workers[workerCount-1]
		prevWorker := cnv.workers[workerCount-2]
		err := LinkWorker2Worker(prevWorker, currWorker) // Linking adjacent worker nodes
		if err != nil {
			return err
		}
	}

	return nil
}

// AddJointWorker employs a new joint station to the conveyor
func (cnv *Conveyor) AddJointWorker(joint JointWorker) error {

	joint.CreateChannels(cnv.bufferLen)
	cnv.joints = append(cnv.joints, joint)

	return nil
}

// Start the Conveyor
func (cnv *Conveyor) Start() error {

	wg := sync.WaitGroup{}

	if cnv.needProgress {
		go cnv.updateProgress()
	}

	for _, nodeWorker := range cnv.workers {
		wg.Add(1)
		go func(nodeWorker NodeWorker) {
			defer wg.Done()
			if err := nodeWorker.Start(cnv.ctx); err != nil {
				log.Println("node worker start failed", err)
				return
			}

			if err := nodeWorker.WaitAndStop(cnv.ctx); err != nil {
				log.Println("node worker stop failed", err)
			}
		}(nodeWorker)
	}

	for _, jointWorker := range cnv.joints {
		wg.Add(1)
		go func(jointWorker JointWorker) {
			defer wg.Done()

			if err := jointWorker.Start(cnv.ctx); err != nil {
				log.Println("join worker start failed", err)
				return
			}

			if err := jointWorker.WaitAndStop(); err != nil {
				log.Println("join worker stop failed", err)
			}

		}(jointWorker)
	}

	// wait for the conveyor to finish
	wg.Wait()

	cnv.cleanup(false) // Cleanup() will be called from here, in case of success or timeout

	return nil
}

// Stop Conveyor by cancelling context. It's used to kill a pipeline while it's running.
// No need to call it if the pipeline is finishing on it's own
func (cnv *Conveyor) Stop() time.Duration {
	// Cancel ctx
	cnv.cleanup(true) // cleanup() will be called from here, in case of killing conveyor
	return cnv.duration
}

// cleanup should be called in all the termination cases: success, kill, & timeout
func (cnv *Conveyor) cleanup(abruptKill bool) {
	// In case, conveyor was killed, ctx.Cancel() night have been already called, but it's an idempotent method
	cnv.ctx.Cancel()
	if cnv.needProgress {
		cnv.cleanupOnce.Do(func() {
			close(cnv.progress)
		})
	}
	if abruptKill == false && cnv.lcHandler != nil {
		if err := cnv.MarkCurrentState(StateFinished); err != nil {
			log.Printf("Conveyor:[%s] unable to set status as 'finished': Error:[%v]\n", cnv.Name, err)
		}
	}
}

func (cnv *Conveyor) updateProgress() {

	start := time.Now()

	ticker := time.NewTicker(cnv.tickProgress)
	defer ticker.Stop()
trackProgress:
	for range ticker.C {
		cnv.duration = time.Since(start)

		percentDone := float64((cnv.duration.Seconds() / cnv.expectedDuration.Seconds()) * 100)
		// if estimate is incorrect, don't overflow progress end
		if percentDone > 100 {
			percentDone = 99.0
		}

		// Seperated out, contex closed check, as in the next check, "default" is always picked.
		// Even if ctx is done, and "cleanup()" function above, has closed "cnv.progress"
		select {
		case <-cnv.ctx.Done():
			break trackProgress
		default:
		}

		select {
		case cnv.progress <- percentDone:
		default:
			<-cnv.progress
			cnv.progress <- percentDone
		}
	}
}

func (cnv *Conveyor) MarkCurrentState(state string) error {
	if cnv.lcHandler == nil {
		return ErrLifeCycleNotSupported
	}

	statusMarkerFunc := getStateMarker(state, cnv.lcHandler)
	if statusMarkerFunc == nil {
		return fmt.Errorf("given state '%s' is not supported by conveyor. "+
			"Look for 'Valid States for a Conveyor' in docs", state)
	}

	return statusMarkerFunc()
}