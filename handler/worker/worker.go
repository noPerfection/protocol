package worker

// Asynchronous replier

import (
	"github.com/sds-framework/handler-lib/base"
	"github.com/sds-framework/handler-lib/config"
	"runtime"
)

// Worker is the socket wrapper for the service.
type Worker struct {
	*base.Handler
	maxInstanceAmount int
}

// New asynchronous replying handler.
func New() *Worker {
	return &Worker{
		Handler:           base.New(),
		maxInstanceAmount: runtime.NumCPU(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Worker) SetConfig(handler *config.Handler) {
	handler.Type = config.WorkerType
	c.Handler.SetConfig(handler)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Worker) Type() config.HandlerType {
	return config.WorkerType
}
