// Package clock provides the process's real ports.Clock implementation.
// Use cases depend on ports.Clock so tests can inject a deterministic one
// instead of touching wall-clock time.
package clock

import "time"

// System implements ports.Clock using the real wall clock.
type System struct{}

func New() System { return System{} }

func (System) Now() time.Time { return time.Now() }
