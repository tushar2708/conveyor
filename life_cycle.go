package conveyor

// Valid States for a Conveyor
const (
	// StatusPreparing status is used to mark a conveyor to be in "preparing" state
	StatusPreparing = "preparing"

	// StateStarted status is used to mark a conveyor to be in "started" state
	StateStarted = "started"

	// StateToKill status is used to mark a conveyor has been setup "to be killed", but isn't yet dead
	StateToKill = "toKill"

	// StateKilled status is used to mark a conveyor as "successfully killed"
	StateKilled = "killed"

	// StateFinished status is used to mark a conveyor as "successfully finished"
	StateFinished = "finished"

	// StateInternalError status is used to indicate that conveyor couldn't finish due to some internal error
	StateInternalError = "internalError"
)

// LifeCycleHandler handles conveyor start/stop
type LifeCycleHandler interface {
	GetState() (string, error)
	GetStatusMsg() (string, error)
	UpdateStatusMsg(string) error
	GetProgress() (string, error)
	UpdateProgress(string) error

	MarkPreparing() error
	MarkStarted() error
	MarkToKill() error
	MarkKilled() error
	MarkFinished() error
	MarkError() error
}

func getStateMarker(state string, lch LifeCycleHandler) func() error {

	switch state {
	case StatusPreparing:
		return lch.MarkPreparing
	case StateStarted:
		return lch.MarkStarted
	case StateToKill:
		return lch.MarkToKill
	case StateKilled:
		return lch.MarkKilled
	case StateFinished:
		return lch.MarkFinished
	case StateInternalError:
		return lch.MarkError
	default:
		return nil
	}

}
