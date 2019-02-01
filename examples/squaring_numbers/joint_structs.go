package squaring_numbers

import "github.com/tushar2708/conveyor"

// ReplicateJoint is a joint executor, which acts as an intermediate node,
// that doesn't change the incoming data, but just replicates it, to multiple nodes
// You can write your own versions of "conveyor.ConcreteJointExecutor", which may:
// 1. Distribute data from one node equally to a set of different nodes.
// 2. Distribute data from one node in fractions.
// 3. Distribute data based on some condition.
// 4. Anything else that you can think of
type ReplicateJoint struct {
	*conveyor.ConcreteJointExecutor
	OutChanCount int
}
