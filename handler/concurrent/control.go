package concurrent

import (
	"fmt"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/control"
	"github.com/noPerfection/protocol/message"
)

const (
	ClosePart      = "close_part"
	RunPart        = "run-part"
	InstanceAmount = "instance-amount"
	MessageAmount  = "message-amount"
	AddInstance    = "add-instance"
	DeleteInstance = "delete-instance"
	Parts          = "parts"
)

// SetControlRoutes registers concurrent-specific control commands.
func (c *Concurrent) SetControlRoutes() error {
	if c.Manager == nil {
		return fmt.Errorf("control manager not initiated")
	}

	routes := map[string]func(message.RequestInterface) message.ReplyInterface{
		control.HandlerStatus: c.onControlStatus,
		control.HandlerClose:  c.onControlClose,
		ClosePart:             c.onClosePart,
		RunPart:               c.onRunPart,
		InstanceAmount:        c.onInstanceAmount,
		MessageAmount:         c.onMessageAmount,
		AddInstance:           c.onAddInstance,
		DeleteInstance:        c.onDeleteInstance,
		Parts:                 c.onParts,
	}
	for command, handle := range routes {
		if err := c.Manager.Route(command, handle); err != nil {
			return fmt.Errorf("control route '%s': %w", command, err)
		}
	}

	return nil
}

func (c *Concurrent) onControlStatus(req message.RequestInterface) message.ReplyInterface {
	frontendStatus := c.Frontend.Status()
	instanceStatus := c.InstanceManager.Status()

	params := datatype.New()
	if frontendStatus == RUNNING && instanceStatus == Running {
		params.Set("status", base.Ready)
	} else {
		params.Set("status", base.Incomplete).
			Set("parts", c.controlPartStatuses())
	}

	return req.Ok(params)
}

func (c *Concurrent) onControlClose(req message.RequestInterface) message.ReplyInterface {
	_ = c.closeControlPart("frontend")
	_ = c.closeControlPart("instance_manager")

	return c.Manager.SetClose(req)
}

func (c *Concurrent) controlPartStatuses() datatype.KeyValue {
	return datatype.New().
		Set("frontend", c.Frontend.Status()).
		Set("instance_manager", c.InstanceManager.Status())
}

func (c *Concurrent) onClosePart(req message.RequestInterface) message.ReplyInterface {
	part, err := req.RouteParameters().StringValue("part")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
	}

	if err := c.closeControlPart(part); err != nil {
		return req.Fail(err.Error())
	}
	return req.Ok(datatype.New())
}

func (c *Concurrent) closeControlPart(part string) error {
	switch part {
	case "frontend":
		if c.Frontend.Status() != RUNNING {
			return fmt.Errorf("frontend not running")
		}
		if err := c.Frontend.Close(); err != nil {
			return fmt.Errorf("failed to close the frontend: %v", err)
		}
	case "instance_manager":
		if c.InstanceManager.Status() != Running {
			return fmt.Errorf("instance_manager not running")
		}
		c.InstanceManager.Close()
	default:
		return fmt.Errorf("unknown part '%s' to stop", part)
	}
	return nil
}

func (c *Concurrent) onRunPart(req message.RequestInterface) message.ReplyInterface {
	part, err := req.RouteParameters().StringValue("part")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
	}

	switch part {
	case "frontend":
		if c.Frontend.Status() == RUNNING {
			return req.Fail("frontend running")
		}
		if err := c.Frontend.Start(); err != nil {
			return req.Fail(fmt.Sprintf("frontend.Start: %v", err))
		}
	case "instance_manager":
		if c.InstanceManager.Status() == Running {
			return req.Fail("instance_manager running")
		}
		if err := c.StartInstanceManager(); err != nil {
			return req.Fail(fmt.Sprintf("c.StartInstanceManager: %v", err))
		}
	default:
		return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
	}

	return req.Ok(datatype.New())
}

func (c *Concurrent) onInstanceAmount(req message.RequestInterface) message.ReplyInterface {
	instanceAmount := len(c.InstanceManager.Instances())
	return req.Ok(datatype.New().Set("instance_amount", instanceAmount))
}

func (c *Concurrent) onMessageAmount(req message.RequestInterface) message.ReplyInterface {
	params := datatype.New().
		Set("queue_length", c.Frontend.QueueLen()).
		Set("processing_length", c.Frontend.ProcessingLen())
	return req.Ok(params)
}

func (c *Concurrent) onAddInstance(req message.RequestInterface) message.ReplyInterface {
	instanceId, err := c.InstanceManager.AddInstance(c.config.HandlerType(), &c.Routes)
	if err != nil {
		return req.Fail(fmt.Sprintf("instanceManager.AddInstance(%s): %v", c.config.HandlerType(), err))
	}

	return req.Ok(datatype.New().Set("instance_id", instanceId))
}

func (c *Concurrent) onDeleteInstance(req message.RequestInterface) message.ReplyInterface {
	instanceId, err := req.RouteParameters().StringValue("instance_id")
	if err != nil {
		return req.Fail(fmt.Sprintf("req.Parameters.GetString('instance_id'): %v", err))
	}

	if err := c.InstanceManager.DeleteInstance(instanceId, false); err != nil {
		return req.Fail(fmt.Sprintf("instanceManager.DeleteInstance('%s'): %v", instanceId, err))
	}

	return req.Ok(datatype.New())
}

func (c *Concurrent) onParts(req message.RequestInterface) message.ReplyInterface {
	parts := []string{
		"frontend",
		"instance_manager",
	}
	messageTypes := []string{
		"queue_length",
		"processing_length",
	}

	params := datatype.New().
		Set("parts", parts).
		Set("message_types", messageTypes)

	return req.Ok(params)
}
