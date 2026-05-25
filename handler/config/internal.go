package config

import (
	"fmt"
	"strings"
)

// UrlToFileName converts the given url to the file name.
// Simply it replaces the slashes with dots.
//
// ExternalUrl returns the full url to connect to the orchestra.
//
// The orchestra url is defined from the main service's url.
//
// For example:
//
//	serviceUrl = "github.com/sds-framework/sample-service"
//	contextUrl = "orchestra.github.com.ahmetson.sample-service"
//
// This url is set as the handler's name in the config.
// Then the handler package will generate an inproc:// or ipc:// url from config.ExternalUrl.
func UrlToFileName(url string) string {
	return strings.ReplaceAll(strings.ReplaceAll(url, "/", "."), "\\", ".")
}

// ParentUrl returns the url of the instance manager
func ParentUrl(handlerId string) string {
	return fmt.Sprintf("inproc://handler_%s", handlerId)
}

// InstanceHandleUrl returns the url of the instance for handling the requests
func InstanceHandleUrl(parentId string, id string) string {
	return fmt.Sprintf("inproc://inst_handle_%s_%s", parentId, id)
}

// InstanceUrl returns the url of the instance for managing the instance itself
func InstanceUrl(parentId string, id string) string {
	return fmt.Sprintf("inproc://inst_manage_%s_%s", parentId, id)
}

// InstanceManagerEventUrl returns a socket that's used to update the instance manager status
func InstanceManagerEventUrl(handlerId string) string {
	return fmt.Sprintf("inproc://inst_manage_stat_%s", handlerId)
}

// NewInternalHandler returns the configuration with the default parameters.
func NewInternalHandler(as HandlerType, id string, category string) *Handler {
	return &Handler{
		Type:           as,
		Category:       category,
		Id:             id,
		InstanceAmount: 1,
		Port:           0,
		ManagerId:      DefaultManagerId(id),
		ManagerPort:    0,
	}
}

// InternalTriggerAble Converts the Handler to Trigger of the given type for internal use
func InternalTriggerAble(handler *Handler, as HandlerType) (*Trigger, error) {
	if !CanTrigger(as) {
		return nil, fmt.Errorf("the '%s' handler type is not trigger-able", as)
	}

	trigger := &Trigger{
		Handler:       handler,
		BroadcastPort: 0,
		BroadcastType: as,
		BroadcastId:   "broadcast_" + handler.Id,
	}

	return trigger, nil
}
