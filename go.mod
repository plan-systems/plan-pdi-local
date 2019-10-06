module github.com/plan-systems/plan-pdi-local

go 1.12

require (
	github.com/dgraph-io/badger v1.6.0
	github.com/plan-systems/plan-core v0.0.3
	google.golang.org/grpc v1.22.0
)

replace github.com/plan-systems/plan-core => ../plan-core
