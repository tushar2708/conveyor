package conveyor

// LinkWorker2Worker links two NodeWorkers, maps input channel of a b on output channel of a
func LinkWorker2Worker(a NodeWorker, b NodeWorker) error {
	ch, err := b.GetInputChannel()
	if err == nil {
		return a.SetOutputChannel(ch)
	}
	return err
}

// LinkJointAfterNode links JointWorker after NodeWorkers, maps input channel of joint worker on output channel of node worker
func LinkJointAfterNode(nw NodeWorker, jw JointWorker, index int) error {
	chnls, err := jw.GetInputChannels()
	if err == nil {
		if index < len(chnls) {
			ch := chnls[index]
			return nw.SetOutputChannel(ch)
		}
		return ErrLessInputChannelsInJoint
	}
	return err
}

// LinkNodeAfterJoint links NodeWorkers after JointWorker, maps input channel of a b on output channel of a
func LinkNodeAfterJoint(jw JointWorker, nw NodeWorker) error {
	ch, err := nw.GetInputChannel()
	if err == nil {
		return jw.AddOutputChannel(ch)
	}
	return err
}
