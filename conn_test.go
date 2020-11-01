package uhatools

import (
	"testing"
)

func TestClient(t *testing.T) {
	cl := OpenCluster(ClusterOptions{
		InitialServers: []string{":11001", ":11002"},
		DialOptions: DialOptions{
			Auth: "hello",
		},
	})
	defer cl.Close()
	c := cl.Get()
	defer c.Close()
}
