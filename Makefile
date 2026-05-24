GOTEST    ?= go test
GOBENCH   ?= go test -bench=. -benchmem -run=^$
GOLINT    ?= go vet
RACEFLAG  ?= -race
PKGS      := ./...
COVERPKGS := $(shell go list ./... | grep -v '/cmd/' | tr '\n' ',' | sed 's/,$$//')

.PHONY: all
all: vet test

.PHONY: test
test:
	$(GOTEST) $(RACEFLAG) -timeout 300s $(PKGS)

.PHONY: test-short
test-short:
	$(GOTEST) $(RACEFLAG) -short -timeout 120s $(PKGS)

.PHONY: cover
cover:
	$(GOTEST) -coverpkg=$(COVERPKGS) -coverprofile=coverage.out -timeout 300s $(PKGS)
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

.PHONY: bench
bench:
	$(GOBENCH) $(PKGS)

.PHONY: vet
vet:
	$(GOLINT) $(PKGS)

.PHONY: fmt
fmt:
	gofmt -s -w .

.PHONY: fmt-check
fmt-check:
	@test -z "$$(gofmt -s -l . | tee /dev/stderr)"

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: clean
clean:
	rm -f coverage.out coverage.html
	rm -rf testdata/scratch
