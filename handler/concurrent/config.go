package concurrent

import (
	"fmt"

	"github.com/noPerfection/protocol/handler/config"
)

// Config is the configuration for handlers that run concurrent instances.
type Config struct {
	*config.Handler
	InstanceAmount uint64 `json:"instance_amount" yaml:"instance_amount"`
}

// NewConfig returns a concurrent handler configuration with a default instance amount.
func NewConfig(as config.HandlerType, id string, category string, port uint64) *Config {
	return &Config{
		Handler:        config.New(as, id, category, port),
		InstanceAmount: 1,
	}
}

// NewInternalConfig returns a concurrent handler configuration for in-process use.
func NewInternalConfig(as config.HandlerType, id string, category string) *Config {
	return &Config{
		Handler:        config.New(as, id, category, 0),
		InstanceAmount: 1,
	}
}

// ParentUrl returns the url of the instance manager.
func ParentUrl(handlerId string) string {
	return fmt.Sprintf("inproc://handler_%s", handlerId)
}

// InstanceHandleUrl returns the url of the instance for handling the requests.
func InstanceHandleUrl(parentId string, id string) string {
	return fmt.Sprintf("inproc://inst_handle_%s_%s", parentId, id)
}

// InstanceUrl returns the url of the instance for managing the instance itself.
func InstanceUrl(parentId string, id string) string {
	return fmt.Sprintf("inproc://inst_manage_%s_%s", parentId, id)
}

// InstanceManagerEventUrl returns a socket that's used to update the instance manager status.
func InstanceManagerEventUrl(handlerId string) string {
	return fmt.Sprintf("inproc://inst_manage_stat_%s", handlerId)
}
