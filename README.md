# plan-pdi-local

```
         P urposeful
         L ogistics
         A rchitecture
P  L  A  N etwork
```

[PLAN](http://plan-systems.org) is a free and open platform for groups to securely communicate, collaborate, and coordinate projects and activities.

## About

This repo is a prototype Persistent Data Interface (PDI) implementation used for development and testing, backed by a local key-value store.


## Building

Requires golang 1.11 or above.

We're in the process of convering this project to use [go modules](https://github.com/golang/go/wiki/Modules). In the meantime, you'll want to checkout this repo into your `GOPATH` (or the default `~/go`).

```
mkdir -p ~/go/src/github.com/plan-systems
cd ~/go/src/github.com/plan-systems
git clone git@github.com:plan-systems/plan-pdi-local.git
cd plan-pnode
go get ./...
go build .
```
