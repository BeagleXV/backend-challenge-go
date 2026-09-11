// Package idgen provides the process's real ports.IDGenerator
// implementation. Use cases depend on ports.IDGenerator so tests can inject
// deterministic IDs instead of random ones.
package idgen

import "github.com/google/uuid"

// UUID implements ports.IDGenerator using random (v4) UUIDs.
type UUID struct{}

func New() UUID { return UUID{} }

func (UUID) NewID() uuid.UUID { return uuid.New() }
