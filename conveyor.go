package conveyor

import (
	"context"
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"sync"
	"time"
)

var (
	// ErrEmptyConveyor error
	ErrEmptyConveyor = errors.New("conveyor is empty, no workers employed")
)

// Conveyor is base
type Conveyor struct {
	id           string
	Name         string
	ctx          CnvContext
	needProgress bool
	tickProgress time.Duration
	bufferLen    int

	progress         chan float64
	duration         time.Duration
	expectedDuration time.Duration
	lifeCycle        LifeCycleHandler

	workers []NodeWorker
	joints  []JointWorker

	startOnce           sync.Once // To ensure that conveyor can't start again
	openForConfigChange bool

	cleanupOnce sync.Once // To ensure that conveyor can't be cleaned up again
}

// NewConveyor creates a new Conveyor instance, with all options set to default values/implementations
func NewConveyor(name string, bufferLen int) (*Conveyor, error) {

	if bufferLen <= 0 {
		bufferLen = 100
	}

	cnv := &Conveyor{
		Name:      name,
		bufferLen: bufferLen,
	}

	// Set ID to a default UUID string
	id := uuid.NewV4()
	cnv.id = id.String()

	// Set expected duration to default value
	cnv.expectedDuration = time.Hour

	// Set lifeCycle to nil (default value)
	cnv.lifeCycle = nil

	// Set needProgress to false by default
	cnv.needProgress = false

	_ctx := &cnvContext{
		Context: context.Background(),
		Data: CtxData{
			Name:   name,
			logs:   make(chan Message, 100),
			status: make(chan string, 100),
		},
	}

	cnv.ctx = _ctx.WithCancel()

	// Keep the conveyor open for config change for now
	// Once this value is turned to false by "lockConfig()", config change will have no effect.
	cnv.openForConfigChange = true

	return cnv, nil
}

func (cnv *Conveyor) lockConfig() {
	cnv.startOnce.Do(func() {
		cnv.openForConfigChange = false
	})
}

// SetID sets the id of Conveyor to a given string
// Will have no effect, once you add your first node
func (cnv *Conveyor) SetID(id string) *Conveyor {
	if cnv.openForConfigChange == false {
		return cnv
	}
	cnv.id = id
	return cnv
}

// SetTimeout sets the timeout of Conveyor to a given value
// Will have no effect, once you add your first node
// If you change the context using "SetCustomContext()" after calling this method,
// timeout will get reset
func (cnv *Conveyor) SetTimeout(timeout time.Duration) *Conveyor {
	if cnv.openForConfigChange == false {
		return cnv
	}
	if timeout <= 0 {
		cnv.ctx = cnv.ctx.WithCancel()
	} else {
		cnv.ctx = cnv.ctx.WithTimeout(timeout)
	}
	return cnv
}

// EnableProgress sets the expectedDuration of Conveyor to a given value.
// Also enables progress based on this value of expectedDuration
// Will have no effect, once you add your first node
func (cnv *Conveyor) EnableProgress(expectedDuration time.Duration) *Conveyor {
	if cnv.openForConfigChange == false {
		return cnv
	}

	cnv.needProgress = true
	cnv.progress = make(chan float64, 1)
	cnv.tickProgress = time.Millisecond * 500

	if expectedDuration == 0 {
		expectedDuration = time.Hour
	}
	cnv.expectedDuration = expectedDuration

	return cnv
}

// SetLifeCycleHandler sets the conveyor's LifeCycleHandler interface to a given implementation
// Will have no effect, once you add your first node
func (cnv *Conveyor) SetLifeCycleHandler(lch LifeCycleHandler) *Conveyor {
	if cnv.openForConfigChange == false {
		return cnv
	}
	cnv.lifeCycle = lch
	return cnv
}

// SetCustomContext sets the conveyor's CnvContext interface to a given implementation
// Will have no effect, once you add your first node
// This method must be called before you call "SetTimeout()"
func (cnv *Conveyor) SetCustomContext(ctx CnvContext) *Conveyor {
	if cnv.openForConfigChange == false {
		return cnv
	}
	cnv.ctx = ctx
	return cnv
}

// GetConveyorContext gives the conveyor's context object
func (cnv *Conveyor) GetConveyorContext() CnvContext {
	return cnv.ctx
}

// Done returns the context.Done() channel of Conveyor
func (cnv *Conveyor) Done() <-chan struct{} {
	return cnv.ctx.Done()
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

// Status returns a channel on which Conveyor Statuses will be published
func (cnv *Conveyor) Status() <-chan string {
	iCtxData := cnv.ctx.GetData()
	if iCtxData != nil {
		ctxData, _ := iCtxData.(CtxData)
		return ctxData.status
	}
	return nil
}

// Progress returns a channel which is regularly updated with progress %
func (cnv *Conveyor) Progress() <-chan float64 {
	if cnv.needProgress {
		return cnv.progress
	}
	return nil
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

	// As a node is now added successfully, can't change configuration anymore
	cnv.lockConfig()

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

	// As a node is now added successfully, can't change configuration anymore
	cnv.lockConfig()

	return nil
}

// AddJointExecutorAfterNode creates a worker for a given executor (based on workerMode & workerType)
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

// AddNodeExecutorAfterJoint creates a worker for a given executor (based on workerMode & workerType)
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

	// As a node is now added successfully, can't change configuration anymore
	cnv.lockConfig()

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

	// As a conveyor is now being started, can't change configuration anymore
	cnv.lockConfig()

	wg := sync.WaitGroup{}

	if cnv.needProgress {
		go cnv.updateProgress()
	}

	workerCount := len(cnv.workers)
	if workerCount == 0 {
		return ErrEmptyConveyor
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
	if abruptKill == false && cnv.lifeCycle != nil {
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

// MarkCurrentState marks the current stage of conveyor using internal life-cycle handler interface
func (cnv *Conveyor) MarkCurrentState(state string) error {
	if cnv.lifeCycle == nil {
		return ErrLifeCycleNotSupported
	}

	statusMarkerFunc := getStateMarker(state, cnv.lifeCycle)
	if statusMarkerFunc == nil {
		return fmt.Errorf("given state '%s' is not supported by conveyor. "+
			"Look for 'Valid States for a Conveyor' in docs", state)
	}

	return statusMarkerFunc()
}
