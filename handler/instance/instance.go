package instance

import (
	"context"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/log-lib"
	"github.com/sds-framework/protocol/client"
	"github.com/sds-framework/protocol/handler/config"
	"github.com/sds-framework/protocol/handler/route"
	"github.com/sds-framework/protocol/message"
	"time"
)

const (
	PREPARE  = "prepare"  // instance is created, but not yet running
	READY    = "ready"    // instance is running and waiting for messages to handle
	HANDLING = "handling" // instance is running, but busy by handling messages
	CLOSED   = "close"    // instance was closed
)

// The Instance is the socket wrapper for the handler instance
//
// The instances have three sockets:
// Push to send its status to the handler
// Sub to receive the messages from the handler
// HandlerType socket to handle the messages.
//
// If parent is PUB
// Then, Instances are per Topic. The instances are sub that receives the messages and then sends it
// to the parent.
//
// If parent is Replier
// Then, the user manages Instances.
//
// The publisher returns two clients.
type Instance struct {
	Id          string
	parentId    string
	handlerType config.HandlerType
	routes      *key_value.KeyValue // handler routing
	routeDeps   *key_value.KeyValue // handler deps
	depClients  *key_value.KeyValue
	messageOps  *message.Operations
	logger      *log.Logger
	close       bool
	status      string // Instance status
}

// New handler of the handlerType
func New(handlerType config.HandlerType, id string, parentId string, parent *log.Logger) *Instance {
	logger := parent.Child(id)

	return &Instance{
		Id:          id,
		parentId:    parentId,
		handlerType: handlerType,
		routes:      nil,
		routeDeps:   nil,
		depClients:  nil,
		logger:      logger,
		close:       false,
		status:      PREPARE,
	}
}

// A reply sends to the caller the message.
//
// If a handler doesn't support replying (for example, PULL handler),
// then it returns success.
func (c *Instance) reply(socket *zmq.Socket, reply message.ReplyInterface) error {
	if !config.CanReply(c.Type()) {
		return nil
	}

	replyStr, err := reply.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("reply.ZmqEnvelope: %w", err)
	}
	if _, err := socket.SendMessage(replyStr); err != nil {
		return fmt.Errorf("recv error replying error %w" + err.Error())
	}

	return nil
}

// Calls handler.reply() with the error message.
func (c *Instance) replyError(socket *zmq.Socket, err error) error {
	return c.reply(socket, c.messageOps.EmptyReq().Fail(err.Error()))
}

// SetRoutes set the reference to the functions and dependencies from the Handler.
func (c *Instance) SetRoutes(routes *key_value.KeyValue, routeDeps *key_value.KeyValue) {
	c.routes = routes
	c.routeDeps = routeDeps
}

// SetClients set the reference to the socket clients
func (c *Instance) SetClients(clients *key_value.KeyValue) {
	c.depClients = clients
}

func (c *Instance) SetMessageOps(ops *message.Operations) {
	c.messageOps = ops
}

// Type returns the type of the instances
func (c *Instance) Type() config.HandlerType {
	return c.handlerType
}

// Status of the instance
func (c *Instance) Status() string {
	return c.status
}

// pubStatus notifies instance manager with the status of this Instance.
func (c *Instance) pubStatus(parent *zmq.Socket, status string) error {
	req := c.messageOps.EmptyReq()
	req.Next("set_status", key_value.New().Set("id", c.Id).Set("status", status))

	reqStr, err := req.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("request.String: %w", err)
	}

	_, err = parent.SendMessageDontwait(reqStr)
	if err != nil {
		return fmt.Errorf("parent.SendMessageDontWait: %w", err)
	}

	return nil
}

// pubFail notifies instance manager that this Instance crashed
func (c *Instance) pubFail(parent *zmq.Socket, instanceErr error) error {
	req := c.messageOps.EmptyReq()
	key_value.New().Set("id", c.Id).Set("status", CLOSED).Set("message", instanceErr.Error())

	reqStr, err := req.ZmqEnvelope()
	if err != nil {
		return fmt.Errorf("request.String: %w", err)
	}

	_, err = parent.SendMessageDontwait(reqStr)
	if err != nil {
		return fmt.Errorf("parent.SendMessageDontWait: %w", err)
	}

	return nil
}

// Start the instance manager and handler in the internal goroutine.
// It waits until the sockets are bound to the endpoints.
// If until the binding occurs an error, then it will return it back.
//
// After binding, the occurred errors are sent back to the instance manager by the pub socket.
//
// If pub socket had an error then, the errors are printed to stderr
func (c *Instance) Start() error {
	if c.messageOps == nil {
		return fmt.Errorf("no message operations. call SetMessageOps")
	}

	ready := make(chan error)

	go func(ready chan error) {
		parent, err := zmq.NewSocket(zmq.PUSH)
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket('PUSH'): %w", err)
			return
		}

		parentUrl := config.ParentUrl(c.parentId)
		err = parent.Connect(parentUrl)
		if err != nil {
			ready <- fmt.Errorf("parent.Connect('%s'): %w", parentUrl, err)
			return
		}

		// Notify the parent that it's getting prepared
		err = c.pubStatus(parent, PREPARE)
		if err != nil {
			ready <- fmt.Errorf("c.pubStatus('%s'): %w", PREPARE, err)
			return
		}
		c.status = PREPARE

		handler, err := zmq.NewSocket(config.SocketType(c.Type()))
		if err != nil {
			ready <- fmt.Errorf("zmq.NewSocket('handler', type: '%s'): %w", c.Type(), err)
			return
		}

		handlerUrl := config.InstanceHandleUrl(c.parentId, c.Id)
		err = handler.Bind(handlerUrl)
		if err != nil {
			ready <- fmt.Errorf("handler.Bind('%s'): %w", handlerUrl, err)
			return
		}

		manager, err := zmq.NewSocket(zmq.REP)
		if err != nil {
			closeErr := handler.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: handler.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("zmq.NewSocket('manager'): %w", err)
			return
		}

		managerUrl := config.InstanceUrl(c.parentId, c.Id)
		err = manager.Bind(managerUrl)
		if err != nil {
			closeErr := handler.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: handler.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("manager.Bind('%s'): %w", managerUrl, err)
			return
		}

		poller := zmq.NewPoller()
		poller.Add(handler, zmq.POLLIN)
		poller.Add(manager, zmq.POLLIN)

		c.status = READY
		c.close = false

		err = c.pubStatus(parent, READY)
		if err != nil {
			closeErr := handler.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: handler.Close: %w", err, closeErr)
			}
			closeErr = manager.Close()
			if closeErr != nil {
				err = fmt.Errorf("%w: manager.Close: %w", err, closeErr)
			}
			ready <- fmt.Errorf("c.pubStatus('%s'): %w", READY, err)
			return
		}

		// exit from instance.Starts
		// the pusher notifies the errors occurred later
		ready <- nil

		// when it receives a close event,
		// the parent might be already closed
		instant := false

		// canceling the handling, if received a close signal
		bgCtx := context.Background()
		var cancelCtx context.Context
		var cancel context.CancelFunc

		for {
			if c.close {
				break
			}

			sockets, err := poller.Poll(time.Millisecond)
			if err != nil {
				newErr := fmt.Errorf("poller.Poll(%s): %w", c.Type(), err)
				pubErr := c.pubFail(parent, newErr)
				if pubErr != nil {
					c.logger.Error("c.pubFail", "argument", newErr, "error", pubErr)
				}
				break
			}

			if len(sockets) == 0 {
				continue
			}

			for _, polled := range sockets {
				if polled.Socket == handler {
					if c.status != READY {
						continue
					}

					data, meta, err := handler.RecvMessageWithMetadata(0)
					if err != nil {
						newErr := fmt.Errorf("socket.recvMessageWithMetadata: %w", err)
						err = c.replyError(handler, newErr)
						if err != nil {
							newErr = fmt.Errorf("c.ReplyError('handler', '%w'): %w", newErr, err)
						}
						pubErr := c.pubFail(parent, newErr)
						if pubErr != nil {
							c.logger.Error("c.pubFail", "argument", newErr, "error", pubErr)
						}
						break
					}

					c.status = HANDLING
					pubErr := c.pubStatus(parent, HANDLING)
					if pubErr != nil {
						// no need to call for c.pubFail, since that could also lead to the same error as pubStatus.
						// they use the same socket.
						c.logger.Error("c.pubStatus", "argument", HANDLING, "error", pubErr)
						break
					}

					cancelCtx, cancel = context.WithCancel(bgCtx)

					go c.processMessage(cancelCtx, cancel, parent, handler, data, meta)
				} else if polled.Socket == manager {
					// The instance manager supports only one command: CLOSE.
					// Therefore, it doesn't have any routes.
					//
					// The manager doesn't notify the instance manager for one reason.
					// If the manager received a CLOSE command, then instance manager might be closed already.
					messages, err := manager.RecvMessage(0)
					if err != nil {
						c.logger.Error("manager.RecvMessage", "error", err)
						break
					}

					req, err := message.NewReq(messages)
					if err != nil {
						c.logger.Error("messageOps.NewReq", "socket", "manager", "messages", messages, "error", err)
						break
					}

					// Assuming the request is close
					// The instant close or not. If it's an instant close, then it won't send the message
					// to the parent.
					instant, err = req.RouteParameters().BoolValue("instant")
					if err != nil {
						c.logger.Error("req.Parameters.GetBoolean", "socket", "manager", "key", "instant", "error", err)
						break
					}

					if !instant {
						// the close parameter is set to true after reply the message back.
						// otherwise, the socket may be closed while the requester is waiting for a reply message.
						// it could leave to the app froze-up.
						reply := &message.Reply{Status: message.OK, Parameters: key_value.New(), Message: ""}
						if err := c.reply(manager, reply); err != nil {
							failErr := fmt.Errorf("c.reply('manager'): %w", err)
							pubErr := c.pubFail(parent, failErr)
							if pubErr != nil {
								c.logger.Error("c.pubFail", "argument", failErr, "error", pubErr)
							}
						}
					}

					// Closing here, rather in the beginning of the loop.
					// Since zmq.Poller takes some time to response.
					// Meanwhile, the processing could be finished.
					if c.status == HANDLING {
						cancel()
						cancel = nil
					}

					// mark as close, we don't exit straight from the loop,
					// because poller may be processing another signal.
					// therefore, adding a close signal to the queue
					c.close = true
					c.status = CLOSED
				}
			}
		}

		err = poller.RemoveBySocket(handler)
		if err != nil {
			if instant {
				c.logger.Error("poller.RemoveBySocket", "socket", "handler", "error", err)
			} else {
				err = fmt.Errorf("poller.RemoveBySocket('handler'): %w", err)
				pubErr := c.pubFail(parent, err)
				if pubErr != nil {
					// we add it to the error stack, but continue to clean out the rest
					// since removing from the poller won't affect the other socket operations.
					c.logger.Error("c.pubFail", "argument", err, "error", pubErr)
				}
			}
		}

		err = poller.RemoveBySocket(manager)
		if err != nil {
			if instant {
				c.logger.Error("poller.RemoveBySocket", "socket", "manager", "error", err)
			} else {
				err = fmt.Errorf("poller.RemoveBySocket('manager'): %w", err)
				pubErr := c.pubFail(parent, err)
				if pubErr != nil {
					// we add it to the error stack, but continue to clean out the rest
					// since removing from the poller won't affect the other socket operations.
					c.logger.Error("c.pubFail", "argument", err, "error", pubErr)
				}
			}
		}

		// if manager closing fails, then restart of this instance will throw an error,
		// since the manager is bound to the endpoint.
		//
		// one option is to remove the thread, and create a new instance with another id.
		err = manager.Close()
		if err != nil {
			if instant {
				c.logger.Error("manager.Close", "error", err)
			} else {
				err = fmt.Errorf("manager.Close: %w", err)
				pubErr := c.pubFail(parent, err)
				if pubErr != nil {
					// we add it to the error stack, but continue to clean out the rest
					// since removing from the poller won't affect the other socket operations.
					c.logger.Error("c.pubFail", "argument", err, "error", pubErr)
				}
			}
		}

		// if manager closing fails, then restart of this instance will throw an error.
		// see above manager.Close comment.
		err = handler.Close()
		if err != nil {
			if instant {
				c.logger.Error("handler.Close", "error", err)
			} else {
				err = fmt.Errorf("handler.Close: %w", err)
				pubErr := c.pubFail(parent, err)
				if pubErr != nil {
					// we add it to the error stack, but continue to clean out the rest
					// since removing from the poller won't affect the other socket operations.
					c.logger.Error("c.pubFail", "argument", err, "error", pubErr)
				}
			}
		}

		if !instant {
			pubErr := c.pubStatus(parent, CLOSED)
			if pubErr != nil {
				c.logger.Error("c.pubStatus", "argument", CLOSED, "error", pubErr)
				// we add it to the error stack, but continue to clean out the rest
				// since removing from the poller won't affect the other socket operations.
			}
		}

		err = parent.Close()
		if err != nil {
			c.logger.Error("parent.Close", "error", err)
		}
	}(ready)

	return <-ready
}

func (c *Instance) handle(reply chan message.ReplyInterface, req message.RequestInterface, handleInterface interface{}, depClients []*client.Socket) {
	result := route.Handle(req, handleInterface, depClients)
	reply <- result
}

func (c *Instance) setReady(parent *zmq.Socket) {
	c.status = READY
	pubErr := c.pubStatus(parent, READY)
	if pubErr != nil {
		// no need to call for c.pubFail, since that could also lead to the same error as pubStatus.
		// they use the same socket.
		c.logger.Error("c.pubStatus", "argument", READY, "error", pubErr)
	}
}

func (c *Instance) processingFinished(parent *zmq.Socket, handler *zmq.Socket, reply message.ReplyInterface) error {
	c.setReady(parent)

	if err := c.reply(handler, reply); err != nil {
		failErr := fmt.Errorf("c.reply('handler'): %w", err)
		pubErr := c.pubFail(parent, failErr)
		if pubErr != nil {
			return fmt.Errorf("c.reply(%v): '%w': c.pubFail: %w", reply, err, pubErr)
		}
	}
	return nil
}

func (c *Instance) processMessage(ctx context.Context, cancel context.CancelFunc, parent *zmq.Socket, handler *zmq.Socket, msgRaw []string, metadata map[string]string) {
	// All request types derive from the basic request.
	// We first attempt to parse basic request from the raw message
	request, err := c.messageOps.NewReq(msgRaw)
	if err != nil {
		c.setReady(parent)

		newErr := fmt.Errorf("message.NewReq (msg len: %d): %w", len(msgRaw), err)

		pubErr := c.pubFail(parent, fmt.Errorf("c.processData: %w", newErr))
		if pubErr != nil {
			c.logger.Error("c.pubFail", "argument", err, "error", pubErr)
		}
		if cancel != nil {
			cancel()
		}
		return
	}
	request.SetMeta(metadata)

	// Add the trace
	//if request.IsFirst() {
	//	request.SetUuid()
	//}
	//request.AddRequestStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id)

	handleInterface, depNames, err := route.Route(request.CommandName(), *c.routes, *c.routeDeps)
	if err != nil {
		reply := request.Fail(fmt.Sprintf("route.Route(%s): %v", request.CommandName(), err))
		finishErr := c.processingFinished(parent, handler, reply)
		if cancel != nil {
			cancel()
		}
		if finishErr != nil {
			c.logger.Error("c.processingFinished", "request_cmd", request.CommandName(), "request_params", request.RouteParameters(), "error", finishErr, "for", err)
		}
		return
	}

	depClients := route.FilterExtensionClients(depNames, *c.depClients)

	reply := make(chan message.ReplyInterface)
	go c.handle(reply, request, handleInterface, depClients)

	// We use a similar pattern to the HTTP server
	// that we saw in the earlier example
	select {
	case result := <-reply:
		// the manager might cancel it.
		if cancel != nil {
			cancel()
			c.processingFinished(parent, handler, result)
		}
	case <-ctx.Done(): // exit without processing
		c.logger.Info("cancel the instance")
	}

	// update the stack
	//if err = reply.SetStack(c.serviceUrl, c.config.Category, c.config.Instances[0].Id); err != nil {
	//	c.logger.Warn("failed to update the reply stack", "error", err)
	//}
}
