package conveyor

// LinkWorker2Worker links two NodeWorkers, maps input channel of a b on output channel of a
func LinkWorker2Worker(a NodeWorker, b NodeWorker) error {
	ch, err := b.GetInputChannel()
	if err == nil {
		return a.SetOutputChannel(ch)
	}
	return err
}

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
