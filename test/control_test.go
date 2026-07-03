package protocoltest

import (
	"testing"
	"time"

	"github.com/noPerfection/protocol/client"
	cpair "github.com/noPerfection/protocol/client/pair"
	cpublisher "github.com/noPerfection/protocol/client/publisher"
	creplier "github.com/noPerfection/protocol/client/replier"
	csyncreplier "github.com/noPerfection/protocol/client/sync_replier"
	cworker "github.com/noPerfection/protocol/client/worker"
	"github.com/noPerfection/protocol/handler/base"
	hcontrol "github.com/noPerfection/protocol/handler/control"
	hpair "github.com/noPerfection/protocol/handler/pair"
	hpublisher "github.com/noPerfection/protocol/handler/publisher"
	hreplier "github.com/noPerfection/protocol/handler/replier"
	hsyncreplier "github.com/noPerfection/protocol/handler/sync_replier"
	hworker "github.com/noPerfection/protocol/handler/worker"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

type controlClient interface {
	HandlerStatus() (string, error)
	HandlerConfig() (client.HandlerConfig, error)
	HandlerClose() error
	StartHandler() (string, error)
	Close() error
}

func TestHandlerControls(t *testing.T) {
	t.Run("sync replier control", func(t *testing.T) {
		svc := hsyncreplier.New()
		endpoint := message.NewEndpoint(testID(t, "sync-control"), 0)
		svc.SetEndpoint(endpoint)
		require.NoError(t, svc.Route("echo", echoRoute))
		require.NoError(t, svc.Start())
		control := newSyncControl(t, endpoint.Id)
		defer func() { require.NoError(t, control.Close()) }()
		assertControlLifecycle(t, control, endpoint)
	})

	t.Run("replier control", func(t *testing.T) {
		svc := hreplier.New()
		endpoint := message.NewEndpoint(testID(t, "replier-control"), 0)
		svc.SetEndpoint(endpoint)
		require.NoError(t, svc.Route("echo", echoRoute))
		require.NoError(t, svc.Start())
		control := newReplierControl(t, endpoint.Id)
		defer func() { require.NoError(t, control.Close()) }()
		assertControlLifecycle(t, control, endpoint)
	})

	t.Run("worker control", func(t *testing.T) {
		svc := hworker.New()
		endpoint := message.NewEndpoint(testID(t, "worker-control"), 0)
		svc.SetEndpoint(endpoint)
		require.NoError(t, svc.Route("work", func(request message.RequestInterface) message.ReplyInterface {
			return request.Ok(request.RouteParameters())
		}))
		require.NoError(t, svc.Start())
		control := newWorkerControl(t, endpoint.Id)
		defer func() { require.NoError(t, control.Close()) }()
		assertControlLifecycle(t, control, endpoint)
	})

	t.Run("pair control", func(t *testing.T) {
		svc := hpair.New()
		endpoint := message.NewEndpoint(testID(t, "pair-control"), 0)
		svc.SetEndpoint(endpoint)
		require.NoError(t, svc.Route("echo", echoRoute))
		require.NoError(t, svc.Start())
		control := newPairControl(t, endpoint.Id)
		defer func() { require.NoError(t, control.Close()) }()
		assertControlLifecycle(t, control, endpoint)
	})

	t.Run("publisher control", func(t *testing.T) {
		svc := hpublisher.New()
		endpoint := message.NewEndpoint(testID(t, "publisher-control"), 0)
		svc.SetEndpoint(endpoint)
		require.NoError(t, svc.Start())
		control := newPublisherControl(t, endpoint.Id)
		defer func() { require.NoError(t, control.Close()) }()
		assertControlLifecycle(t, control, endpoint)
	})
}

func assertControlLifecycle(t *testing.T, control controlClient, endpoint message.Endpoint) {
	t.Helper()
	req := require.New(t)

	status, err := control.HandlerStatus()
	req.NoError(err)
	req.Equal(base.SocketReady, status)

	handlerConfig, err := control.HandlerConfig()
	req.NoError(err)
	req.Equal(endpoint.Id, handlerConfig.Id)
	req.Equal(endpoint.Port, handlerConfig.Port)

	req.NoError(control.HandlerClose())
	waitForStatus(t, control, base.SocketNil)

	status, err = control.StartHandler()
	req.NoError(err)
	req.Equal(base.SocketReady, status)
	waitForStatus(t, control, base.SocketReady)

	req.NoError(control.HandlerClose())
	waitForStatus(t, control, base.SocketNil)
}

func waitForStatus(t *testing.T, control controlClient, expected string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.Tick(10 * time.Millisecond)
	for {
		status, err := control.HandlerStatus()
		if err == nil && status == expected {
			return
		}
		select {
		case <-deadline:
			require.NoError(t, err)
			require.Equal(t, expected, status)
			return
		case <-tick:
		}
	}
}

func newSyncControl(t *testing.T, id string) *csyncreplier.Control {
	t.Helper()
	control, err := csyncreplier.NewControl(controlID(id, 0), 0)
	require.NoError(t, err)
	control.Timeout(time.Second)
	control.Attempt(3)
	return control
}

func newReplierControl(t *testing.T, id string) *creplier.Control {
	t.Helper()
	control, err := creplier.NewControl(controlID(id, 0), 0)
	require.NoError(t, err)
	control.Timeout(time.Second)
	control.Attempt(3)
	return control
}

func newWorkerControl(t *testing.T, id string) *cworker.Control {
	t.Helper()
	control, err := cworker.NewControl(controlID(id, 0), 0)
	require.NoError(t, err)
	control.Timeout(time.Second)
	control.Attempt(3)
	return control
}

func newPairControl(t *testing.T, id string) *cpair.Control {
	t.Helper()
	control, err := cpair.NewControl(controlID(id, 0), 0)
	require.NoError(t, err)
	control.Timeout(time.Second)
	control.Attempt(3)
	return control
}

func newPublisherControl(t *testing.T, id string) *cpublisher.Control {
	t.Helper()
	control, err := cpublisher.NewControl(controlID(id, 0), 0)
	require.NoError(t, err)
	control.Timeout(time.Second)
	control.Attempt(3)
	return control
}

func controlID(id string, port uint64) string {
	return hcontrol.NewInternalControlEndpoint(message.NewEndpoint(id, port)).Id
}
