package route

import (
	"fmt"
	"github.com/sds-framework/datatype-lib/data_type/key_value"
	"github.com/sds-framework/protocol/client"
)

// Any route name
const Any string = "*"

// FilterExtensionClients returns the list of the clients specific for this route
func FilterExtensionClients(deps []string, clients key_value.KeyValue) []*client.Socket {
	routeClients := make([]*client.Socket, len(deps))

	for i := 0; i < len(deps); i++ {
		found := false

		for extensionName := range clients {
			if deps[i] == extensionName {
				routeClients[i] = clients[extensionName].(*client.Socket)
				found = true
				break
			}
		}

		if !found {
			routeClients[i] = nil
		}
	}

	return routeClients
}

// Route finds the dependencies and the handling function for the given command.
//
// Note that in golang, returning interfaces are considered a bad practice.
// However, we do still return an interface{} as this interface will be a different type of Route.
//
// Returns handle func from the func list, returns dependencies list and error
func Route(cmd string, routeFuncs key_value.KeyValue, routeDeps key_value.KeyValue) (interface{}, []string, error) {
	var handleInterface interface{}
	var handleDeps []string
	var err error

	if routeFuncs.Exist(cmd) {
		handleInterface = routeFuncs[cmd]
		if routeDeps.Exist(cmd) {
			handleDeps, err = routeDeps.StringsValue(cmd)
		} else {
			err = nil
		}
	} else {
		if routeFuncs.Exist(Any) {
			handleInterface = routeFuncs[Any]
			if routeDeps.Exist(Any) {
				handleDeps, err = routeDeps.StringsValue(Any)
			} else {
				err = nil
			}
		} else {
			err = fmt.Errorf("the '%s' command handler not found", cmd)
		}
	}

	return handleInterface, handleDeps, err
}
