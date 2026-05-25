module github.com/sds-framework/protocol/handler

go 1.19

require (
	github.com/pebbe/zmq4 v1.2.10
	github.com/sds-framework/datatype-lib v0.0.0-20260519113206-6acc97659255
	github.com/sds-framework/log-lib v0.0.0-20260519113119-b6fe63f7315e
	github.com/sds-framework/protocol/client v0.0.0
	github.com/sds-framework/protocol/message v0.0.0
	github.com/stretchr/testify v1.8.4
)

replace (
	github.com/sds-framework/protocol/client => ../client
	github.com/sds-framework/protocol/message => ../message
)

require (
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/charmbracelet/lipgloss v0.8.0 // indirect
	github.com/charmbracelet/log v0.2.4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logfmt/logfmt v0.6.0 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/muesli/clusters v0.0.0-20200529215643-2700303c1762 // indirect
	github.com/muesli/gamut v0.3.1 // indirect
	github.com/muesli/kmeans v0.3.1 // indirect
	github.com/muesli/reflow v0.3.0 // indirect
	github.com/muesli/termenv v0.15.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	golang.org/x/sys v0.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
