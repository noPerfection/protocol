module github.com/sds-framework/protocol/client

go 1.19

require github.com/stretchr/testify v1.8.4

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/pebbe/zmq4 v1.2.10
	github.com/sds-framework/datatype-lib v0.0.0-20260519113206-6acc97659255
	github.com/sds-framework/protocol/message v0.0.0
)

replace github.com/sds-framework/protocol/message => ../message
