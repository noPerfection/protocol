package sync_replier

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
)

type SyncReplier struct {
	*base.Handler
	handlerType config.HandlerType
}

// New SyncReplier returned
func New() *SyncReplier {
	handler := base.New()
	return &SyncReplier{
		Handler:     handler,
		handlerType: config.SyncReplierType,
	}
}

// SetConfig adds the parameters of the handler from the config.
func (c *SyncReplier) SetConfig(handler *config.Handler) {
	handler.Type = config.SyncReplierType
	c.Handler.SetConfig(handler)
}

// Type returns the handler type. If the configuration is not set, returns config.UnknownType.
func (c *SyncReplier) Type() config.HandlerType {
	return config.SyncReplierType
}

// Start the handler directly, not by goroutine
func (c *SyncReplier) Start() error {
	onAddInstance := func(req message.RequestInterface) message.ReplyInterface {
		if len(c.InstanceManager.Instances()) != 0 {
			return req.Fail("only one instance allowed in sync replier")
		}

		instanceId, err := c.InstanceManager.AddInstance(c.Config().Type, &c.Routes)
		if err != nil {
			return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", c.Config().Type, err))
		}

		params := datatype.New().Set("instance_id", instanceId)
		return req.Ok(params)
	}

	if c.Manager == nil {
		return fmt.Errorf("handler manager not initited. call SetConfig and SetLogger first")
	}

	if err := c.Manager.Route(config.AddInstance, onAddInstance); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}

	return c.Handler.Start()
}
