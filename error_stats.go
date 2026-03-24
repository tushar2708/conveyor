package conveyor

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrorStats tracks pipeline errors by total count and by stage+root-error-type bucket.
// All methods are safe for concurrent use.
type ErrorStats struct {
	total  atomic.Int64
	byType sync.Map // key: "StageName:*pkg.ErrorType" → *atomic.Int64
}

// Record increments the total error count and the per-type bucket.
// The map key is "{stage}:{root error type}" where the root error type is obtained
// by unwrapping all fmt.Errorf("%w", ...) layers to find the innermost error type.
func (es *ErrorStats) Record(stage string, err error) {
	es.total.Add(1)
	key := stage + ":" + rootErrorType(err)
	v, _ := es.byType.LoadOrStore(key, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

// Total returns the total number of errors recorded.
func (es *ErrorStats) Total() int64 {
	return es.total.Load()
}

// Snapshot returns a point-in-time copy of the per-type error counts.
// Safe to read without external synchronization.
func (es *ErrorStats) Snapshot() map[string]int64 {
	result := make(map[string]int64)
	es.byType.Range(func(k, v any) bool {
		result[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return result
}

// rootErrorType walks errors.Unwrap() until the innermost (base) error is found
// and returns its type as a string (e.g., "*fs.PathError", "*pgconn.PgError").
// This ensures multi-level wrapped errors are always bucketed by their root cause.
func rootErrorType(err error) string {
	for {
		unwrapped := errors.Unwrap(err)
		if unwrapped == nil {
			return fmt.Sprintf("%T", err)
		}
		err = unwrapped
	}
}
