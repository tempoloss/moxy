package core

// State describes the lifecycle position of a task.
type State string

const (
	Ready    State = "ready"
	Leased   State = "leased"
	Acked    State = "acked"
	Expired  State = "expired"
	Requeued State = "requeued"
)
