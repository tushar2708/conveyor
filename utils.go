package conveyor

import "errors"

func sendStatus(ctx *CnvContext, status string) error {

	select {
	case <-ctx.Done():
		return nil
	case ctx.Data.status <- status:
	default:
		<-ctx.Data.status // If not consumed, throw away old status and update with new value
		ctx.Data.status <- status
	}
	return nil
}

func sendLogs(ctx *CnvContext, text string, err error) error {

	logEntry := Message{Text: text, Err: err}

	select {
	case <-ctx.Done():
		return nil
	case ctx.Data.logs <- logEntry:
	default:
		ctx.Data.logs <- logEntry
	}
	return nil
}

// LinkWorker2Worker links two NodeWorkers, maps input channel of a b on output channel of a
func LinkWorker2Worker(a NodeWorker, b NodeWorker) error {
	ch, err := b.GetInputChannel()
	if err == nil {
		return a.SetOutputChannel(ch)
	}
	return err
}

// ErrLessInputChannelsInJoint error
var ErrLessInputChannelsInJoint = errors.New("JointWorker doesn't have enough input channels")

// ErrLessOutputChannelsInJoint error
var ErrLessOutputChannelsInJoint = errors.New("JointWorker doesn't have enough output channels")

// LinkWorker2Joint links two NodeWorkers, maps input channel of a b on output channel of a
func LinkWorker2Joint(a NodeWorker, b JointWorker, index int) error {
	chnls, err := b.GetInputChannels()
	if err == nil {
		if index < len(chnls) {
			ch := chnls[index]
			return a.SetOutputChannel(ch)
		}
		return ErrLessInputChannelsInJoint
	}
	return err
}

// LinkJoint2Worker links two NodeWorkers, maps input channel of a b on output channel of a
func LinkJoint2Worker(a JointWorker, b NodeWorker, index int) error {
	ch, err := b.GetInputChannel()
	if err == nil {
		return a.AddOutputChannel(ch)
	}
	return err
}
