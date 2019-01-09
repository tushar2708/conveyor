package conveyor

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// Conveyor to run the graph
type Conveyor struct {
	Name         string
	ctx          *CnvContext
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
	return cnv.ctx.Data.status
}

// GetConveyorContext gives the conveyor's context object
func (cnv *Conveyor) GetConveyorContext() *CnvContext {
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
	return cnv.ctx.Data.logs
}

var (
	// ErrEmptyConveyor error
	ErrEmptyConveyor = errors.New("conveyor is empty, no workers employed")
)

// New creates a new Conveyor instance
func New(name string, bufferLen int) (*Conveyor, error) {

	conveyor, err := newConveyor(name, bufferLen, nil, -1)
	conveyor.needProgress = false

	return conveyor, err
}

// NewTimeout creates a new Conveyor instance with specified timeout
func NewTimeout(name string, bufferLen int, timeout time.Duration) (*Conveyor, error) {

	conveyor, err := newConveyor(name, bufferLen, nil, timeout)
	conveyor.needProgress = false

	return conveyor, err
}

// NewTimeoutAndProgress creates a new Conveyor instance
func NewTimeoutAndProgress(name string, bufferLen int, lch LifeCycleHandler, timeout, expectedDuration time.Duration) (*Conveyor, error) {

	if expectedDuration == 0 {
		expectedDuration = time.Hour
	}

	conveyor, err := newConveyor(name, bufferLen, lch, timeout)

	conveyor.needProgress = true
	conveyor.progress = make(chan float64, 1)

	conveyor.expectedDuration = expectedDuration
	conveyor.tickProgress = time.Millisecond * 500

	return conveyor, err
}

func newConveyor(name string, bufferLen int, lch LifeCycleHandler, timeout time.Duration) (*Conveyor, error) {

	if bufferLen <= 0 {
		bufferLen = 100
	}

	_ctx := &CnvContext{
		Context: context.Background(),
		Data: CtxData{
			Name:   name,
			logs:   make(chan Message, 100),
			status: make(chan string, 1),
		},
	}
	var ctx *CnvContext
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

// AddWorker employs a new worker station to the conveyor
func (cnv *Conveyor) AddWorker(worker NodeWorker, toLink bool) error {
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

// Stop Conveyor by cancelling context. It's used to kill a campaign while it's running.
// No need to call it if campaign is finishing on it's own
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
	if abruptKill == false {
		if err := cnv.MarkFinished(); err != nil {
			log.Printf("Campaign:[%s] unable to set status as 'finished' to redis: %v\n", cnv.Name, err)
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

// MarkPreparing marks conveyor status as "preparing"
func (cnv *Conveyor) MarkPreparing() error {
	if cnv.lcHandler == nil {
		return ErrLifeCycleNotSupported
	}
	return cnv.lcHandler.MarkPreparing()
}

// MarkStarted marks conveyor status as "started"
func (cnv *Conveyor) MarkStarted() error {
	if cnv.lcHandler == nil {
		return ErrLifeCycleNotSupported
	}
	return cnv.lcHandler.MarkStarted()
}

// MarkToKill marks conveyor status as "to kill"
func (cnv *Conveyor) MarkToKill() error {
	if cnv.lcHandler == nil {
		return ErrLifeCycleNotSupported
	}
	return cnv.lcHandler.MarkToKill()
}

// MarkKilled marks conveyor status as "killed"
func (cnv *Conveyor) MarkKilled() error {
	if cnv.lcHandler == nil {
		return ErrLifeCycleNotSupported
	}
	return cnv.lcHandler.MarkKilled()
}

// MarkFinished marks conveyor status as "finished"
func (cnv *Conveyor) MarkFinished() error {
	if cnv.lcHandler == nil {
		return ErrLifeCycleNotSupported
	}
	return cnv.lcHandler.MarkFinished()
}

// MarkError marks conveyor status as "internal error"
func (cnv *Conveyor) MarkError() error {
	if cnv.lcHandler == nil {
		return ErrLifeCycleNotSupported
	}
	return cnv.lcHandler.MarkError()
}
