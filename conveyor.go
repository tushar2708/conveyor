package conveyor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/satori/go.uuid"
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

	lastNodeOutType  reflect.Type
	lastJointOutType reflect.Type

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
	if !cnv.openForConfigChange {
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
	if !cnv.openForConfigChange {
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
	if !cnv.openForConfigChange {
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
	if !cnv.openForConfigChange {
		return cnv
	}
	cnv.lifeCycle = lch
	return cnv
}

// SetCustomContext sets the conveyor's CnvContext interface to a given implementation
// Will have no effect, once you add your first node
// This method must be called before you call "SetTimeout()"
func (cnv *Conveyor) SetCustomContext(ctx CnvContext) *Conveyor {
	if !cnv.openForConfigChange {
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

// AddSource adds a source node to the conveyor. It must be the first node added.
// TOut is the type of data this source produces.
//
// The executor is wrapped into a type-erased adapter, a SourceWorkerPool is created
// for it, and the pool is appended to the conveyor's worker list. After a successful
// call, cnv.lastNodeOutType is set to reflect.TypeFor[TOut]() so that the next
// AddOperation or AddSink call can validate its input type at construction time.
func AddSource[TOut any](cnv *Conveyor, exec SourceExecutor[TOut], mode WorkerMode) error {
	wrapped := wrapSource[TOut](exec)
	workerType := WorkerTypeSource

	nodeWorker, err := newNodeWorker(wrapped, mode, workerType)
	if err != nil {
		return err
	}

	if addErr := cnv.AddNodeWorker(nodeWorker, true); addErr != nil {
		fmt.Printf("Adding %s [type:%s] to conveyor failed. Error:[%v]\n",
			exec.GetName(), workerType, addErr)
		return addErr
	}

	cnv.lastNodeOutType = reflect.TypeFor[TOut]()
	cnv.lockConfig()
	return nil
}

// AddOperation adds an operation node to the conveyor.
// TIn must match the output type of the previously added node; a type mismatch
// returns ErrTypeMismatch at construction time before any workers are started.
// TOut is the type this operation produces; it is recorded so the next node can
// validate its own input type.
func AddOperation[TIn, TOut any](cnv *Conveyor, exec OperationExecutor[TIn, TOut], mode WorkerMode) error {
	expectedIn := reflect.TypeFor[TIn]()
	if cnv.lastNodeOutType != nil && cnv.lastNodeOutType != expectedIn {
		return fmt.Errorf("%w: expected input type %v but got %v", ErrTypeMismatch, cnv.lastNodeOutType, expectedIn)
	}

	wrapped := wrapOperation[TIn, TOut](exec)
	workerType := WorkerTypeOperation

	nodeWorker, err := newNodeWorker(wrapped, mode, workerType)
	if err != nil {
		return err
	}

	if addErr := cnv.AddNodeWorker(nodeWorker, true); addErr != nil {
		fmt.Printf("Adding %s [type:%s] to conveyor failed. Error:[%v]\n",
			exec.GetName(), workerType, addErr)
		return addErr
	}

	cnv.lastNodeOutType = reflect.TypeFor[TOut]()
	cnv.lockConfig()
	return nil
}

// AddSink adds a sink node to the conveyor.
// TIn must match the output type of the previously added node; a type mismatch
// returns ErrTypeMismatch at construction time before any workers are started.
// After a sink is added, cnv.lastNodeOutType is cleared to nil because sinks
// produce no output for a subsequent node to consume.
func AddSink[TIn any](cnv *Conveyor, exec SinkExecutor[TIn], mode WorkerMode) error {
	expectedIn := reflect.TypeFor[TIn]()
	if cnv.lastNodeOutType != nil && cnv.lastNodeOutType != expectedIn {
		return fmt.Errorf("%w: expected input type %v but got %v", ErrTypeMismatch, cnv.lastNodeOutType, expectedIn)
	}

	wrapped := wrapSink[TIn](exec)
	workerType := WorkerTypeSink

	nodeWorker, err := newNodeWorker(wrapped, mode, workerType)
	if err != nil {
		return err
	}

	if addErr := cnv.AddNodeWorker(nodeWorker, true); addErr != nil {
		fmt.Printf("Adding %s [type:%s] to conveyor failed. Error:[%v]\n",
			exec.GetName(), workerType, addErr)
		return addErr
	}

	cnv.lastNodeOutType = nil // sinks produce no output
	cnv.lockConfig()
	return nil
}

// AddJointAfterNode adds a joint executor after the last node in the conveyor.
// TIn must match the output type of the last node added via AddSource or AddOperation;
// a mismatch returns ErrTypeMismatch. ErrNoNodesAvailable is returned when no node
// has been added yet.
//
// The joint's input channel index 0 is linked to the last node's output channel.
// After a successful call, cnv.lastJointOutType is set to reflect.TypeFor[TOut]()
// and cnv.lastNodeOutType is cleared so that subsequent nodes must be attached
// through AddSinkAfterJoint or AddOperationAfterJoint.
func AddJointAfterNode[TIn, TOut any](cnv *Conveyor, exec JointExecutor[TIn, TOut]) error {
	expectedIn := reflect.TypeFor[TIn]()
	if cnv.lastNodeOutType != nil && cnv.lastNodeOutType != expectedIn {
		return fmt.Errorf("%w: expected input type %v but got %v", ErrTypeMismatch, cnv.lastNodeOutType, expectedIn)
	}

	nodeCount := len(cnv.workers)
	if nodeCount == 0 {
		return ErrNoNodesAvailable
	}

	wrapped := wrapJoint[TIn, TOut](exec)
	jointWorker := NewJointWorkerPool(wrapped)

	if addErr := cnv.AddJointWorker(jointWorker); addErr != nil {
		fmt.Printf("Adding joint-%s after node to conveyor failed. Error:[%v]\n",
			exec.GetName(), addErr)
		return addErr
	}

	// Link the last node's output channel to input channel index 0 of the joint.
	nodeWorker := cnv.workers[nodeCount-1]
	if linkErr := LinkJointAfterNode(nodeWorker, jointWorker, 0); linkErr != nil {
		return linkErr
	}

	// After a joint, nodes must be added via AddSinkAfterJoint / AddOperationAfterJoint,
	// not through the linear AddOperation / AddSink path.
	cnv.lastNodeOutType = nil
	cnv.lastJointOutType = reflect.TypeFor[TOut]()
	cnv.lockConfig()
	return nil
}

// AddSinkAfterJoint adds a sink node after the last joint in the conveyor.
// TIn must match the output type of the last joint added via AddJointAfterNode;
// a mismatch returns ErrTypeMismatch. ErrNoJointsAvailable is returned when no
// joint has been added yet.
//
// The node is appended to the worker list without automatic channel linking
// (toLink=false) because its input channel is provided by the joint's fan-out,
// not by the previous node in the linear chain.
func AddSinkAfterJoint[TIn any](cnv *Conveyor, exec SinkExecutor[TIn], mode WorkerMode) error {
	expectedIn := reflect.TypeFor[TIn]()
	if cnv.lastJointOutType != nil && cnv.lastJointOutType != expectedIn {
		return fmt.Errorf("%w: expected input type %v but got %v", ErrTypeMismatch, cnv.lastJointOutType, expectedIn)
	}

	jointCount := len(cnv.joints)
	if jointCount == 0 {
		return ErrNoJointsAvailable
	}

	wrapped := wrapSink[TIn](exec)
	workerType := WorkerTypeSink

	nodeWorker, err := newNodeWorker(wrapped, mode, workerType)
	if err != nil {
		return err
	}

	// Add to the worker list but skip automatic node-to-node linking; the joint
	// will supply this node's input channel via LinkNodeAfterJoint below.
	if addErr := cnv.AddNodeWorker(nodeWorker, false); addErr != nil {
		fmt.Printf("Adding sink-%s after joint to conveyor failed. Error:[%v]\n",
			exec.GetName(), addErr)
		return addErr
	}

	// Wire a new output channel of the last joint to this node's input channel.
	jointWorker := cnv.joints[jointCount-1]
	if linkErr := LinkNodeAfterJoint(jointWorker, nodeWorker); linkErr != nil {
		return linkErr
	}

	cnv.lockConfig()
	return nil
}

// AddOperationAfterJoint adds an operation node after the last joint in the conveyor.
// TIn must match the output type of the last joint added via AddJointAfterNode;
// a mismatch returns ErrTypeMismatch. ErrNoJointsAvailable is returned when no
// joint has been added yet.
//
// Like AddSinkAfterJoint, the node is appended without automatic channel linking
// and is instead wired to the joint's next available output channel.
// cnv.lastNodeOutType is updated to TOut so that further linear nodes can be
// chained after this operation if needed.
func AddOperationAfterJoint[TIn, TOut any](cnv *Conveyor, exec OperationExecutor[TIn, TOut], mode WorkerMode) error {
	expectedIn := reflect.TypeFor[TIn]()
	if cnv.lastJointOutType != nil && cnv.lastJointOutType != expectedIn {
		return fmt.Errorf("%w: expected input type %v but got %v", ErrTypeMismatch, cnv.lastJointOutType, expectedIn)
	}

	jointCount := len(cnv.joints)
	if jointCount == 0 {
		return ErrNoJointsAvailable
	}

	wrapped := wrapOperation[TIn, TOut](exec)
	workerType := WorkerTypeOperation

	nodeWorker, err := newNodeWorker(wrapped, mode, workerType)
	if err != nil {
		return err
	}

	// Add to the worker list but skip automatic node-to-node linking; the joint
	// will supply this node's input channel via LinkNodeAfterJoint below.
	if addErr := cnv.AddNodeWorker(nodeWorker, false); addErr != nil {
		fmt.Printf("Adding operation-%s after joint to conveyor failed. Error:[%v]\n",
			exec.GetName(), addErr)
		return addErr
	}

	// Wire a new output channel of the last joint to this node's input channel.
	jointWorker := cnv.joints[jointCount-1]
	if linkErr := LinkNodeAfterJoint(jointWorker, nodeWorker); linkErr != nil {
		return linkErr
	}

	cnv.lastNodeOutType = reflect.TypeFor[TOut]()
	cnv.lockConfig()
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
	if !abruptKill && cnv.lifeCycle != nil {
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
