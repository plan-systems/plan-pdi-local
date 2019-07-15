MAKEFLAGS += --warn-undefined-variables
SHELL = /bin/bash -o nounset -o errexit -o pipefail
.DEFAULT_GOAL = build

repo := plan-pdi-local

# ----------------------------------------
# dev/test

.PHONY: check fmt lint test

check: fmt lint test

fmt:
	go fmt ./...

lint:
	go vet ./...

test:
	go test -v ./...

build: build/$(repo)

build/$(repo):
	mkdir -p build
	go build -o build/$(repo) .

.PHONY: clean
clean:
	rm -rf build dist $(repo)

# ----------------------------------------
# release artifacts

TARBALLS := dist/$(repo)_linux_amd64.tar.gz dist/$(repo)_darwin_amd64.tar.gz dist/$(repo)_windows_amd64.tar.gz

.PHONY: release
release: $(TARBALLS)
	cp -R dist ..

build/linux_amd64/$(repo):
	@mkdir -p build
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$@"

build/darwin_amd64/$(repo):
	@mkdir -p build
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o "$@"

build/windows_amd64/$(repo).exe:
	@mkdir -p build
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o "$@"

dist/$(repo)_linux_amd64.tar.gz: build/linux_amd64/$(repo)
	@mkdir -p dist
	tar -czf "$@" $<

dist/$(repo)_darwin_amd64.tar.gz: build/darwin_amd64/$(repo)
	@mkdir -p dist
	tar -czf "$@" $<

dist/$(repo)_windows_amd64.tar.gz: build/windows_amd64/$(repo).exe
	@mkdir -p dist
	tar -czf "$@" $<

# ----------------------------------------
# multi-repo workspace

# uncomments 'replace' flags in go.mod so that we can use a locally
# cloned version of plan-core for development. marks changes to the
# go.mod and go.sum files as temporarily ignored by git as well.
.PHONY: hack
hack:
	if [ ! -d ../plan-core ]; then echo "* Missing ../plan-core. Exiting."; exit 1; fi
	sed -i'' 's~^// replace~replace~' go.mod
	git update-index --assume-unchanged go.mod
	git update-index --assume-unchanged go.sum

# comments-out 'replace' flags in go.mod to undo the work above
.PHONY: unhack
unhack:
	sed -i'' 's~^replace~// replace~' go.mod
	git update-index --no-assume-unchanged go.mod
	git update-index --no-assume-unchanged go.sum
