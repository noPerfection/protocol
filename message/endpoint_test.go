package message

import "testing"

func TestEndpointHandlerUrl(t *testing.T) {
	tests := []struct {
		name     string
		endpoint Endpoint
		want     string
	}{
		{name: "tcp", endpoint: NewEndpoint("sample", 6000), want: "tcp://sample:6000"},
		{name: "tcp localhost", endpoint: NewEndpoint("localhost", 6000), want: "tcp://*:6000"},
		{name: "tcp loopback", endpoint: NewEndpoint("127.0.0.1", 6000), want: "tcp://*:6000"},
		{name: "inproc", endpoint: NewEndpoint("my-service", 0), want: "inproc://my-service"},
		{name: "ipc tmp", endpoint: NewEndpoint("tmp-test.sock", 0), want: "ipc:///tmp-test.sock"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.endpoint.HandlerUrl(); got != test.want {
				t.Fatalf("HandlerUrl() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEndpointClientUrl(t *testing.T) {
	tests := []struct {
		name     string
		endpoint Endpoint
		want     string
	}{
		{name: "tcp", endpoint: NewEndpoint("sample", 6000), want: "tcp://sample:6000"},
		{name: "tcp localhost", endpoint: NewEndpoint("localhost", 6000), want: "tcp://localhost:6000"},
		{name: "tcp loopback", endpoint: NewEndpoint("127.0.0.1", 6000), want: "tcp://localhost:6000"},
		{name: "inproc", endpoint: NewEndpoint("my-service", 0), want: "inproc://my-service"},
		{name: "ipc tmp", endpoint: NewEndpoint("tmp-test.sock", 0), want: "ipc:///tmp-test.sock"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.endpoint.ClientUrl(); got != test.want {
				t.Fatalf("ClientUrl() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEndpointKinds(t *testing.T) {
	inproc := NewEndpoint("my-service", 0)
	if !inproc.IsInproc() || inproc.IsIpc() || inproc.IsRemote() {
		t.Fatalf("expected inproc endpoint, got %+v", inproc)
	}

	ipc := NewEndpoint("tmp-test.sock", 0)
	if !ipc.IsIpc() || ipc.IsInproc() || ipc.IsRemote() {
		t.Fatalf("expected ipc endpoint, got %+v", ipc)
	}

	localhost := NewEndpoint("localhost", 6000)
	if !localhost.IsLocalhost() || !localhost.IsRemote() {
		t.Fatalf("expected localhost remote endpoint, got %+v", localhost)
	}
}
