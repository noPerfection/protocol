package base

import (
	"testing"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/message"
	"github.com/stretchr/testify/require"
)

func TestMisc(t *testing.T) {
	handler := New()
	require.Empty(t, handler.Commands())
	require.IsType(t, &message.MessagePacker{}, handler.Packer())

	require.NoError(t, AnyRoute(handler))
	require.NotEmpty(t, handler.Commands())
}

func TestPacker(t *testing.T) {
	handler := New()
	packer := message.RawMessage()

	handler.SetPacker(packer)

	require.Same(t, packer, handler.Packer())
}

func TestGetHandleFunc(t *testing.T) {
	handler := New()
	handleAny := func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}
	handleCmd := func(request message.RequestInterface) message.ReplyInterface {
		return request.Ok(datatype.New())
	}

	_, err := handler.GetHandleFunc("cmd")
	require.Error(t, err)

	require.NoError(t, handler.Route(Any, handleAny))
	handle, err := handler.GetHandleFunc("cmd")
	require.NoError(t, err)
	require.NotNil(t, handle)

	require.NoError(t, handler.Route("cmd", handleCmd))
	handle, err = handler.GetHandleFunc("cmd")
	require.NoError(t, err)
	require.NotNil(t, handle)
}
