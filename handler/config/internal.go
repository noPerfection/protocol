package config

import (
	"strings"

	"github.com/noPerfection/protocol/message"
)

// UrlToFileName converts the given url to the file name.
// Simply it replaces the slashes with dots.
//
// HandlerUrl returns the full url to connect to the orchestra.
//
// The orchestra url is defined from the main service's url.
//
// For example:
//
//	serviceUrl = "github.com/sds-framework/sample-service"
//	contextUrl = "orchestra.github.com.ahmetson.sample-service"
//
// This url is set as the handler's name in the config.
// Then the handler package will generate an inproc:// or ipc:// url from Endpoint.HandlerUrl.
func UrlToFileName(url string) string {
	return strings.ReplaceAll(strings.ReplaceAll(url, "/", "."), "\\", ".")
}

// NewInternalHandler returns the configuration with the default parameters.
func NewInternalHandler(as HandlerType, id string, category string) *Handler {
	return &Handler{
		Type:     as,
		Category: category,
		Endpoint: message.NewEndpoint(id, 0),
	}
}
