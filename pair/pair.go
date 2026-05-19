// Package pair adds a layer that forwards incoming messages to the instances and vice versa.
package pair

import (
	"fmt"
	"github.com/sds-framework/client-lib"
	clientConfig "github.com/sds-framework/client-lib/config"
	"github.com/sds-framework/handler-lib/config"
)

// Config converts the external handler config into the paired one
func Config(originalConfig *config.Handler) *config.Handler {
	pairConfig := &config.Handler{
		Type:           config.PairType,
		Category:       originalConfig.Category + "_pair",
		Id:             originalConfig.Id + "_pair",
		InstanceAmount: originalConfig.InstanceAmount,
		Port:           0,
	}
	return pairConfig
}

// NewClient returns a client that's connected to the external socket.
// Requires the original handler configuration.
// The Client will create a pair socket parameters using Config.
func NewClient(originalConfig *config.Handler) (*client.Socket, error) {
	externalConfig := Config(originalConfig)
	pairConfig := clientConfig.New("", externalConfig.Id, externalConfig.Port, config.SocketType(externalConfig.Type))
	pairConfig.UrlFunc(clientConfig.Url)

	pairClient, err := client.New(pairConfig)
	if err != nil {
		return nil, fmt.Errorf("client.New: %w", err)
	}
	return pairClient, nil
}
