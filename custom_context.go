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

// CnvContext is a wrapper over context.Context
// To avoid sacrificing type checking with context.WithValue() wherever it's not needed
type CnvContext struct {
	context.Context

	cancelOnce sync.Once
	Data       CtxData
}

// WithCancel is a wrapper on context.WithCancel() for CnvContext type,
// that also copies the Data to new context
func (ctx *CnvContext) WithCancel() *CnvContext {
	newctx, cancel := context.WithCancel(ctx.Context)
	cnvContext := &CnvContext{
		Context: newctx,
		Data:    ctx.Data,
	}
	cnvContext.Data.cancelProgress = cancel

	return cnvContext
}

// WithTimeout is a wrapper on context.WithTimeout() for CnvContext type,
// that also copies the Data to new context
func (ctx *CnvContext) WithTimeout(timeout time.Duration) *CnvContext {
	newctx, cancel := context.WithTimeout(ctx.Context, timeout)
	cnvContext := &CnvContext{
		Context: newctx,
		Data:    ctx.Data,
	}
	cnvContext.Data.cancelProgress = cancel

	return cnvContext
}

// Cancel the derived context, along with closing internal channels
// It has been made to follow the "non-panic multiple cancel" behaviour of built-in context
func (ctx *CnvContext) Cancel() {
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
func (ctx *CnvContext) SendLog(logLevel int32, text string, err error) {
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
	}
}

// SendStatus sends conveyor's internal logs to be available on conveyor.Status()
func (ctx *CnvContext) SendStatus(status string) {

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
