package task

// Task represents a unit of work managed by Moxy.
type Task struct {
	ID       string `json:"id"`
	Payload  []byte `json:"payload"`
	Attempts int    `json:"attempts"`
}
