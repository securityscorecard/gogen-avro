package container

import (
	"io"

	stats "github.com/securityscorecard/go-stats"
)

type AvroRecord interface {
	Serialize(io.Writer) error
	Schema() string
	SendStats(stats.Statser)
}
