package route

import (
	"fmt"
	"github.com/noPerfection/datatype"
)

// Any route name
const Any string = "*"

// Route finds the handling function for the given command.
//
// Note that in golang, returning interfaces are considered a bad practice.
// However, we do still return an interface{} as this interface will be a different type of Route.
//
// Returns handle func from the func list and error
func Route(cmd string, routeFuncs datatype.KeyValue) (interface{}, error) {
	var handleInterface interface{}
	var err error

	if routeFuncs.Exist(cmd) {
		handleInterface = routeFuncs[cmd]
	} else {
		if routeFuncs.Exist(Any) {
			handleInterface = routeFuncs[Any]
		} else {
			err = fmt.Errorf("the '%s' command handler not found", cmd)
		}
	}

	return handleInterface, err
}
