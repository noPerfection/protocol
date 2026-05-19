// Package instance_manager manages the instances
package instance_manager

import (
	"fmt"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/datatype-lib/message"
	"github.com/sds-framework/handler-lib/config"
	zmq "github.com/pebbe/zmq4"
)

const (
	EventClose           = "close"            // notify instance manager is starting to close
	EventIdle            = "idle"             // notify instance manager is not running but created
	EventReady           = "ready"            // notify instance manager is ready
	EventError           = "error"            // notify if in the instance manager occurred an error
	EventInstanceAdded   = "instance_added"   // notify if a new instance added
	EventInstanceDeleted = "instance_deleted" // notify if the instance is deleted
)

// Broadcast that instance manager received a close signal
func (parent *Parent) pubIdle(closeSignal bool) error {
	parameters := key_value.New().Set("close", closeSignal)
	if err := parent.pubEvent(EventIdle, parameters); err != nil {
		return fmt.Errorf("parent.pubEvent('idle'): %w", err)
	}
	return nil
}

func (parent *Parent) pubReady() error {
	parameters := key_value.New()
	if err := parent.pubEvent(EventReady, parameters); err != nil {
		return fmt.Errorf("parent.pubEvent('ready'): %w", err)
	}
	return nil
}

func (parent *Parent) pubClose() error {
	parameters := key_value.New()
	if err := parent.pubEvent(EventClose, parameters); err != nil {
		return fmt.Errorf("parent.pubEvent('ready'): %w", err)
	}
	return nil
}

func (parent *Parent) pubError() error {
	parameters := key_value.New().Set("message", parent.status)
	if err := parent.pubEvent(EventError, parameters); err != nil {
		return fmt.Errorf("parent.pubEvent('error'): %w", err)
	}
	return nil
}

func (parent *Parent) pubInstanceAdded(id string) error {
	parameters := key_value.New().Set("id", id)
	if err := parent.pubEvent(EventInstanceAdded, parameters); err != nil {
		return fmt.Errorf("parent.pubEvent('error'): %w", err)
	}
	return nil
}

func (parent *Parent) pubInstanceDeleted(id string) error {
	parameters := key_value.New().Set("id", id)
	if err := parent.pubEvent(EventInstanceDeleted, parameters); err != nil {
		return fmt.Errorf("parent.pubEvent('error'): %w", err)
	}
	return nil
}

func (parent *Parent) pubEvent(event string, parameters key_value.KeyValue) error {
	if parent.eventSock == nil {
		return fmt.Errorf("event sock not set")
	}
	req := message.Request{Command: event, Parameters: parameters}
	reqStr, err := req.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("req.String: %w", err)
	}

	_, err = parent.eventSock.SendMessage(reqStr)
	if err != nil {
		return fmt.Errorf("eventSock.SendMessageDontWait: %w", err)
	}

	return nil
}

func (parent *Parent) newEventSocket() (*zmq.Socket, error) {
	eventSock, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return nil, fmt.Errorf("zmq.NewSocket(zmq.PUB): %w", err)
	}

	eventUrl := config.InstanceManagerEventUrl(parent.id)
	err = eventSock.Bind(eventUrl)
	if err != nil {
		return nil, fmt.Errorf("eventSock.Bind('%s'): %w", eventUrl, err)
	}

	return eventSock, nil
}
