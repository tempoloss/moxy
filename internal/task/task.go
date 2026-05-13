package task

// Task represents a unit of work managed by Moxy.
type Task struct {
	ID      string
	Payload []byte
}
