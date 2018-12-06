package conveyor

const (
	// StatusPreparing status value
	StatusPreparing = "preparing"
	// StateStarted status value
	StateStarted = "started"
	// StateInternalError status value
	StateInternalError = "internalError"
	// StateFinished status value
	StateFinished = "finished"
	// StateToKill status value
	StateToKill = "toKill"
	// StateKilled status value
	StateKilled = "killed"
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
