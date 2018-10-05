package conveyor

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// Message struct stores one unit of message that conveyor passed back for logging
type Message struct {
	Err  error
	Text string
}

// Conveyor to run the graph
type Conveyor struct {
	Name string
	ctx  *CnvContext

	progress         chan float64
	duration         time.Duration
	expectedDuration time.Duration

	workers []NodeWorker
	joints  []JointWorker

	// SourceNode NodeWorker
	// InnerNodes []NodeWorker
	// SinkNodes  []NodeWorker
}

// Status returns a channel on which Conveyor Statuses will be published
func (cnv *Conveyor) Status() <-chan string {
	return cnv.ctx.Data.status
}

// Done returns the context.Done() channel of Conveyor
func (cnv *Conveyor) Done() <-chan struct{} {
	return cnv.ctx.Done()
}

// Progress returns a channel which is regularly updated with progress %
func (cnv *Conveyor) Progress() <-chan float64 { return cnv.progress }

// Logs returns a channel on which Conveyor Statuses will be published
func (cnv *Conveyor) Logs() <-chan Message {
	return cnv.ctx.Data.logs
}

var (
	// ErrEmptyConveyor error
	ErrEmptyConveyor = errors.New("conveyor is emty, no workers employed")
)

// New creates a new Conveyor instance
func New(name string, expectedDuration time.Duration) (*Conveyor, error) {

	if expectedDuration == 0 {
		expectedDuration = time.Hour
	}

	_ctx := &CnvContext{
		Context: context.Background(),
		Data: CtxData{
			Name:   name,
			logs:   make(chan Message, 100),
			status: make(chan string, 1),
		},
	}
	ctx, cancel := _ctx.WithCancel()
	ctx.Data.cancelProgress = cancel

	cnv := &Conveyor{
		Name:             name,
		expectedDuration: expectedDuration,
		progress:         make(chan float64, 1),
		ctx:              ctx,
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

	cnv.joints = append(cnv.joints, joint)

	return nil
}

// Start the Conveyor
func (cnv *Conveyor) Start() error {

	wg := sync.WaitGroup{}

	for _, nodeWorker := range cnv.workers {
		wg.Add(1)
		go func(nodeWorker NodeWorker) {
			defer wg.Done()
			if err := nodeWorker.Start(cnv.ctx); err != nil {
				log.Println("node worker start failed", err)
				return
			}

			if err := nodeWorker.WaitAndStop(); err != nil {
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

	return nil
}

// Stop Conveyor by cancelling context. It's used to kill a campaign while it's running.
// No need to call it if campaign is finishing on it's own
func (cnv *Conveyor) Stop() error {
	// Cancel ctx
	cnv.ctx.Data.cancelProgress()
	return nil
}

func (cnv *Conveyor) updateProgress(ctx context.Context, ticker *time.Ticker) {
	start := time.Now()
loop:
	for range ticker.C {
		cnv.duration = time.Since(start)

		percentDone := float64((cnv.duration.Seconds() / cnv.expectedDuration.Seconds()) * 100)
		// if estimate is incorrect don't overflow progress end
		if percentDone > 100 {
			percentDone = 99.0
		}

		select {
		case <-ctx.Done(): // add a timeout context
			break loop
		case cnv.progress <- percentDone:
		default:
			<-cnv.progress
			cnv.progress <- percentDone
		}
	}
}

// CtxData stores the information that is stored inside a conveyor, useful for it's lifecycle.ConveyorData
// Any fields only useful for initialization shouldn't be here
type CtxData struct {
	Name string

	logs           chan Message
	status         chan string
	cancelProgress context.CancelFunc
	// cancelAll      context.CancelFunc

}

// CnvContext is a wrapper over context.Context
// To avoid sacrificing type checking with context.WithValue() wherever it's not needed
type CnvContext struct {
	context.Context
	Data CtxData
}

// WithCancel is a wrapper on context.WithCancel() for CnvContext type,
// that also copies the Data to new context
func (ctx *CnvContext) WithCancel() (*CnvContext, func()) {
	newctx, cancel := context.WithCancel(ctx.Context)
	return &CnvContext{
		Context: newctx,
		Data:    ctx.Data,
	}, cancel
}
