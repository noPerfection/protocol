package client

import (
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

// Secure stores the CURVE server public key (Z85). An empty key keeps the client non-secure.
// An optional client secret key may be passed; when set, its public key is derived with AuthCurvePublic.
// When omitted, an ephemeral client keypair is generated on each reconnect.
// CURVE is applied only when the endpoint is not inproc; inproc endpoints skip it.
func (socket *Socket) Secure(serverPublicKey string, clientSecretKey ...string) *Socket {
	socket.mu.Lock()
	socket.serverPublicKey = serverPublicKey
	socket.curveSecretKey = ""
	if len(clientSecretKey) > 0 {
		socket.curveSecretKey = clientSecretKey[0]
	}
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
