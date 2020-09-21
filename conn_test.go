package uhatools

import (
	"testing"
)

func TestClient(t *testing.T) {
	cl := NewCluster(ClusterOptions{
		Addresses: []string{":11001", ":11002"},
		Auth:      "hello",
	})
	defer cl.Release()
	c := cl.Get()
	defer c.Release()
}
