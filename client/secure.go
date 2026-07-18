package client

import (
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

// Secure stores the CURVE client secret key (Z85). When empty, an ephemeral
// client keypair is generated on each reconnect.
func (socket *Socket) Secure(clientSecretKey string) *Socket {
	socket.mu.Lock()
	socket.curveSecretKey = clientSecretKey
	socket.mu.Unlock()
	return socket
}

// Allow stores the CURVE server public key (Z85). An empty key keeps the client non-secure.
func (socket *Socket) Allow(handlerPublicKey string) *Socket {
	socket.mu.Lock()
	socket.serverPublicKey = handlerPublicKey
	socket.mu.Unlock()
	return socket
}

func (socket *Socket) applyCurveClient(zmqSocket *zmq.Socket) error {
	if socket.serverPublicKey == "" || socket.endpoint.IsInproc() {
		return nil
	}

	clientSecret := socket.curveSecretKey
	var clientPublic string
	var err error

	if clientSecret != "" {
		clientPublic, err = zmq.AuthCurvePublic(clientSecret)
		if err != nil {
			return fmt.Errorf("zmq.AuthCurvePublic: %w", err)
		}
	} else {
		clientPublic, clientSecret, err = zmq.NewCurveKeypair()
		if err != nil {
			return fmt.Errorf("zmq.NewCurveKeypair: %w", err)
		}
	}

	if err := zmqSocket.ClientAuthCurve(socket.serverPublicKey, clientPublic, clientSecret); err != nil {
		return fmt.Errorf("socket.ClientAuthCurve: %w", err)
	}

	return nil
}
