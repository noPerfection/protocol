package worker

// Asynchronous replier

import (
	"runtime"

	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
)

// Worker is the socket wrapper for the service.
type Worker struct {
	*base.Concurrent
	maxInstanceAmount int
}

// New asynchronous replying handler.
func New() *Worker {
	return &Worker{
		Concurrent:        base.NewConcurrent(),
		maxInstanceAmount: runtime.NumCPU(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Worker) SetConfig(handler *config.Concurrent) {
	handler.Type = config.WorkerType
	c.Concurrent.SetConfig(handler)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Worker) Type() config.HandlerType {
	return config.WorkerType
}
