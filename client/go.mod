module github.com/noPerfection/protocol/client

go 1.19

require github.com/stretchr/testify v1.11.1

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/noPerfection/datatype v0.1.0
	github.com/noPerfection/protocol/message v0.0.0
	github.com/pebbe/zmq4 v1.2.10
)

replace github.com/noPerfection/protocol/message => ../message
