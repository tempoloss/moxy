package core

import "time"

// Lease represents temporary ownership of a task by a worker.
type Lease struct {
	LeaseID   string
	Task      Task
	ExpiresAt time.Time
	CreatedAt time.Time
}
