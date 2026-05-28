package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMisc(t *testing.T) {
	require.Len(t, requiredMetadata(), 2)

	handler := New()
	require.Empty(t, handler.Routes)

	require.NoError(t, AnyRoute(handler))
	require.NotEmpty(t, handler.Routes)
}
