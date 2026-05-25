package trigger

import (
	clientConfig "github.com/sds-framework/client-lib/config"
	"github.com/sds-framework/handler-lib/config"
)

type Interface interface {
	TriggerClient() *clientConfig.Client
	Config() *config.Trigger
}
