GO    ?= go
GOFMT ?= gofmt

.PHONY: test fmt

test:
	$(GO) vet ./...
	$(GO) test -race ./...
	$(GO) run ./tools/nocomments $$($(GO) list -f '{{.Dir}}' ./...)
	@out="$$($(GOFMT) -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

fmt:
	$(GOFMT) -w .
