package conveyor

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Message struct stores one unit of message that conveyor passed back for logging
type Message struct {
	Text     string
	LogLevel int32
	// Err      error
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

// CnvContext is an interface, which is satisfied by CnvContext.
// This interface is primarily to enabling mocking for unit-testing.CnvContextAble
// Or may be, something fancy that you might want to do.
type CnvContext interface {
	context.Context
	WithCancel() CnvContext
	WithTimeout(time.Duration) CnvContext
	Cancel()
	SendLog(int32, string, error)
	SendStatus(string)
	GetData() interface{}
}

// cnvContext is a wrapper over context.Context
// To avoid sacrificing type checking with context.WithValue() wherever it's not needed
type cnvContext struct {
	context.Context

	cancelOnce sync.Once
	Data       CtxData
}

// GetData
func (ctx *cnvContext) GetData() interface{} {
	return ctx.Data
}

// WithCancel is a wrapper on context.WithCancel() for CnvContext type,
// that also copies the Data to new context
func (ctx *cnvContext) WithCancel() CnvContext {
	newctx, cancel := context.WithCancel(ctx.Context)
	cnvContext := &cnvContext{
		Context: newctx,
		Data:    ctx.Data,
	}
	cnvContext.Data.cancelProgress = cancel

	return cnvContext
}

// WithTimeout is a wrapper on context.WithTimeout() for CnvContext type,
// that also copies the Data to new context
func (ctx *cnvContext) WithTimeout(timeout time.Duration) CnvContext {
	newctx, cancel := context.WithTimeout(ctx.Context, timeout)
	cnvContext := &cnvContext{
		Context: newctx,
		Data:    ctx.Data,
	}
	cnvContext.Data.cancelProgress = cancel

	return cnvContext
}

// Cancel the derived context, along with closing internal channels
// It has been made to follow the "non-panic multiple cancel" behaviour of built-in context
func (ctx *cnvContext) Cancel() {
	if ctx.Data.cancelProgress != nil {
		ctx.Data.cancelProgress()
	}

	ctx.cancelOnce.Do(func() {
		if ctx.Data.logs != nil {
			close(ctx.Data.logs)
		}

		if ctx.Data.logs != nil {
			close(ctx.Data.status)
		}
	})

}

// SendLog sends conveyor's internal logs to be available on conveyor.Logs()
func (ctx *cnvContext) SendLog(logLevel int32, text string, err error) {
	if err != nil {
		text = fmt.Sprintf("conveyor: %s, [err: %s]\n", text, err)
	} else {
		text = fmt.Sprintf("conveyor: %s\n", text)
	}
	msg := Message{
		LogLevel: logLevel,
		Text:     text,
	}

	select {
	case <-ctx.Done():
		return
	default:
	}

	select {
	case <-ctx.Done():
		return
	case ctx.Data.logs <- msg:
	default:
		<-ctx.Data.logs // If not consumed, throw away old status and update with new value
		ctx.Data.logs <- msg
	}
}

// SendStatus sends conveyor's internal logs to be available on conveyor.Status()
func (ctx *cnvContext) SendStatus(status string) {

	select {
	case <-ctx.Done():
		return
	default:
	}

	select {
	case <-ctx.Done():
		return
	case ctx.Data.status <- status:
	default:
		<-ctx.Data.status // If not consumed, throw away old status and update with new value
		ctx.Data.status <- status
	}
}
