package command

const (
	EnqueueName = "MOXY.ENQUEUE"
	FetchName   = "MOXY.FETCH"
	AckName     = "MOXY.ACK"
	StatsName   = "MOXY.STATS"
)

// Command is a protocol-neutral Moxy command.
type Command struct {
	Name string
	Args []string
}
