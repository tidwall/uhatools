package uhatools

import (
	"fmt"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	cl := NewCluster(ClusterOptions{
		Addresses: []string{":11001", ":11002"},
		Auth:      "hello",
	})
	defer cl.Release()

	c := cl.Get()
	defer c.Release()

	// c, err := Dial(":11001", "hello", nil)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// defer c.Close()
	for {
		v, err := c.Do("select count(*) from user")
		if err != nil {
			println(err.Error())
		} else {
			fmt.Printf("%s\n", v)
		}
		time.Sleep(time.Second / 2)
	}

}
