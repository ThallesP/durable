package memstore_test

import (
	"testing"

	"github.com/ThallesP/durable"
	"github.com/ThallesP/durable/durabletest"
	"github.com/ThallesP/durable/memstore"
)

func TestStore(t *testing.T) {
	durabletest.Store(t, func(*testing.T) durable.Store { return memstore.New() })
}
