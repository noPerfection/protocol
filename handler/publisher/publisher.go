package publisher

import (
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/trigger"
)

type Publisher struct {
	*trigger.Trigger
}

// New Publisher returned
func New() *Publisher {
	handler := trigger.New()
	return &Publisher{
		handler,
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *Publisher) SetConfig(trigger *config.Trigger) {
	trigger.Type = config.PublisherType
	c.Trigger.SetConfig(trigger)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *Publisher) Type() config.HandlerType {
	return config.PublisherType
}
