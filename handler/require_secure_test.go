package handler

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	protocolClient "github.com/noPerfection/protocol/client"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/require"
)

const sampleRoute = "sample"

func TestRequireSecure(t *testing.T) {
	if !zmq.HasCurve() {
		t.Skip("CURVE not available in this libzmq build")
	}

	t.Run("AlreadySecureBeforeStart", func(t *testing.T) {
		testRequireSecureAlreadySecureBeforeStart(t)
	})

	t.Run("SecureAfterStartViaControl", func(t *testing.T) {
		testRequireSecureAfterStartViaControl(t)
	})
}

func testRequireSecureAlreadySecureBeforeStart(t *testing.T) {
	serverPublic, serverSecret, err := message.GenerateCurveKey()
	require.NoError(t, err)

	clientPublic, clientSecret, err := message.GenerateCurveKey()
	require.NoError(t, err)

	handler, endpoint, control := startSampleSyncReplier(t, func(h *SyncReplier) {
		h.Secure(serverSecret)
		h.Allow(clientPublic)
	})
	defer stopSampleSyncReplier(t, handler, control)

	external := newSampleSyncReplierClient(t, endpoint, serverPublic, clientSecret)
	requireSampleRouteOK(t, external)

	returnedPublic, err := control.RequireSecure()
	require.NoError(t, err)
	require.Equal(t, serverPublic, returnedPublic)

	requireSampleRouteOK(t, external)
}

func testRequireSecureAfterStartViaControl(t *testing.T) {
	clientPublic, clientSecret, err := message.GenerateCurveKey()
	require.NoError(t, err)

	handler, endpoint, control := startSampleSyncReplier(t, nil)
	defer stopSampleSyncReplier(t, handler, control)

	plainClient := newSampleSyncReplierClient(t, endpoint, "", "")
	requireSampleRouteOK(t, plainClient)

	returnedPublic, err := control.RequireSecure()
	require.NoError(t, err)
	require.NotEmpty(t, returnedPublic)

	_, err = plainClient.Request(sampleRequest())
	require.Error(t, err)
	require.True(t,
		errors.Is(err, message.ErrNoCurveKey) ||
			strings.Contains(err.Error(), message.ErrNoCurveKey.Error()) ||
			strings.Contains(err.Error(), "ErrNoCurveKey"),
		"expected ErrNoCurveKey, got: %v", err)

	handler.Allow(clientPublic)

	secureClient := newSampleSyncReplierClient(t, endpoint, returnedPublic, clientSecret)
	requireSampleRouteOK(t, secureClient)
}

func TestSecureOutbound(t *testing.T) {
	if !zmq.HasCurve() {
		t.Skip("CURVE not available in this libzmq build")
	}

	handler, endpoint, control := startSampleSyncReplier(t, nil)
	defer stopSampleSyncReplier(t, handler, control)

	plainClient := newSampleSyncReplierClient(t, endpoint, "", "")
	requireSampleRouteOK(t, plainClient)

	returnedPublic, err := control.SecureOutbound()
	require.NoError(t, err)
	require.NotEmpty(t, returnedPublic)

	requireSampleRouteOK(t, plainClient)

	returnedAgain, err := control.SecureOutbound()
	require.NoError(t, err)
	require.Equal(t, returnedPublic, returnedAgain)
}

func startSampleSyncReplier(
	t *testing.T,
	configure func(*SyncReplier),
) (*SyncReplier, message.Endpoint, *protocolClient.Control) {
	t.Helper()

	endpoint := tcpTestEndpoint(t)
	testID := fmt.Sprintf("%s_%d", strings.ReplaceAll(t.Name(), "/", "_"), endpoint.Port)

	handler := NewSyncReplier()
	handler.SetEndpoint(endpoint)
	handler.SetMushroomURL(testMushroomURL(testID))
	require.NoError(t, handler.Route(sampleRoute, func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("route", sampleRoute))
	}))

	if configure != nil {
		configure(handler)
	}

	require.NoError(t, handler.Start())
	time.Sleep(100 * time.Millisecond)

	controlEndpoint := NewInternalControlEndpoint(endpoint)
	control, err := protocolClient.NewControl(controlEndpoint.Id, controlEndpoint.Port)
	require.NoError(t, err)

	return handler, endpoint, control
}

func stopSampleSyncReplier(t *testing.T, handler *SyncReplier, control *protocolClient.Control) {
	t.Helper()

	if control != nil {
		_ = control.HandlerClose()
		_ = control.Close()
	}

	if handler != nil && handler.Control != nil && handler.Control.Running() {
		handler.Control.SetSocketNil()
		if wake := handler.wake; wake != nil {
			wake.signal()
		}
		handler.workW.Wait()
	}
}

func tcpTestEndpoint(t *testing.T) message.Endpoint {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint64(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())

	return message.NewEndpoint("127.0.0.1", port)
}

func newSampleSyncReplierClient(
	t *testing.T,
	endpoint message.Endpoint,
	serverPublicKey string,
	clientSecretKey string,
) *protocolClient.SyncReplierClient {
	t.Helper()

	client, err := protocolClient.NewSyncReplier(endpoint.Id, endpoint.Port)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	client.Timeout(5 * time.Second)
	client.Attempt(2)

	if serverPublicKey != "" {
		client.Allow(serverPublicKey)
	}
	if clientSecretKey != "" {
		client.Secure(clientSecretKey)
	}

	return client
}

func sampleRequest() *message.Request {
	return &message.Request{
		Command:    sampleRoute,
		Parameters: datatype.New().Set("probe", true),
	}
}

func requireSampleRouteOK(t *testing.T, client *protocolClient.SyncReplierClient) {
	t.Helper()

	reply, err := client.Request(sampleRequest())
	require.NoError(t, err)
	require.True(t, reply.IsOK(), reply.ErrorMessage())

	route, err := reply.ReplyParameters().StringValue("route")
	require.NoError(t, err)
	require.Equal(t, sampleRoute, route)
}
