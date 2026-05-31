package base

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestMisc(t *testing.T) {
	handler := New()
	require.Empty(t, handler.RouteCommands())
	require.IsType(t, &message.MessagePacker{}, handler.Packer())

	require.NoError(t, AnyRoute(handler))
	require.NotEmpty(t, handler.RouteCommands())
}

func TestPacker(t *testing.T) {
	handler := New()
	packer := message.RawMessage()

	handler.SetPacker(packer)

	require.Same(t, packer, handler.Packer())
}

func TestFindRoute(t *testing.T) {
	handlers := datatype.New()
	handleAny := func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}
	handleCmd := func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}

	_, err := FindRoute("cmd", handlers)
	require.Error(t, err)

	handlers.Set(Any, handleAny)
	handle, err := FindRoute("cmd", handlers)
	require.NoError(t, err)
	require.NotNil(t, handle)

	handlers.Set("cmd", handleCmd)
	handle, err = FindRoute("cmd", handlers)
	require.NoError(t, err)
	require.NotNil(t, handle)
}

func TestHandle(t *testing.T) {
	req := &message.Request{
		Command:    "ping",
		Parameters: datatype.New(),
	}

	reply := Handle(req, func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	})

	require.True(t, reply.IsOK())
}
