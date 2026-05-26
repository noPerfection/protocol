package trigger

import (
	clientConfig "github.com/noPerfection/protocol/client/config"
	"github.com/noPerfection/protocol/handler/config"
)

type Interface interface {
	TriggerClient() *clientConfig.Client
	Config() *config.Trigger
}
