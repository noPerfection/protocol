package trigger

import (
	clientConfig "github.com/sds-framework/protocol/client/config"
	"github.com/sds-framework/protocol/handler/config"
)

type Interface interface {
	TriggerClient() *clientConfig.Client
	Config() *config.Trigger
}
