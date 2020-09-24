package uhatools

import (
	"crypto/tls"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/btree"
)

var defaultLeadershipTimeout = time.Second * 20
var defaultConnectionTimeout = time.Second * 5
var defaultRetryTimeout = time.Millisecond * 100
var defaultPoolSize = 3

// ErrConnectionReleased is returned when a Uhaha connection has been released.
var ErrConnectionReleased = errors.New("connection released")

// ErrLeadershipTimeout is returned when a conenction to a Uhaha leader could
// not be established within the required time.
var ErrLeadershipTimeout = errors.New("leadership timeout")

// ErrConnectionTimeout is returned when a conenction to a Uhaha server could
// not be established within the required time.
var ErrConnectionTimeout = errors.New("connection timeout")

// Cluster represents a Uhaha cluster
type Cluster struct {
	mu       sync.RWMutex
	opts     ClusterOptions
	released bool
	pool     *btree.BTree
}

// ClusterOptions are provide to NewCluster.
type ClusterOptions struct {
	Addresses         []string      // required: one or more
	Auth              string        // optional
	TLSConfig         *tls.Config   // optional
	PoolSize          int           // default: 15
	ConnectionTimeout time.Duration // default: 5s
	LeadershipTimeout time.Duration // default: 20s
}

// NewCluster defines a new Cluster
func NewCluster(opts ClusterOptions) *Cluster {
	cl := new(Cluster)
	cl.opts = opts
	cl.opts.Addresses = append([]string{}, opts.Addresses...)
	if cl.opts.TLSConfig != nil {
		cl.opts.TLSConfig = cl.opts.TLSConfig.Clone()
	}
	if cl.opts.PoolSize == 0 {
		cl.opts.PoolSize = defaultPoolSize
	}
	if cl.opts.ConnectionTimeout == 0 {
		cl.opts.ConnectionTimeout = defaultConnectionTimeout
	}
	if cl.opts.LeadershipTimeout == 0 {
		cl.opts.LeadershipTimeout = defaultLeadershipTimeout
	}
	cl.pool = btree.New(32)
	return cl
}

// Release a cluster and close and pooled connection
func (cl *Cluster) Release() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.released {
		return
	}
	for cl.pool.Len() > 0 {
		(*Conn)(cl.pool.DeleteMax().(*connItem)).Release()
	}
	cl.released = true
}

// Get a connection from the Cluster pool.
func (cl *Cluster) Get() *Conn {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	if cl.released {
		return &Conn{released: true}
	}
	if cl.pool.Len() == 0 {
		return &Conn{cl: cl}
	}
	c := (*Conn)(cl.pool.DeleteMax().(*connItem))
	c.released = false
	return c
}

func (cl *Cluster) getConnectionInfo(leader bool) (addr, auth string,
	tlscfg *tls.Config,
) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	if len(cl.opts.Addresses) > 0 {
		addr = cl.opts.Addresses[rand.Int()%len(cl.opts.Addresses)]
	}
	return addr, cl.opts.Auth, cl.opts.TLSConfig
}

type connItem Conn

func (c *connItem) Less(item btree.Item) bool {
	return c.lastDo.Before(item.(*connItem).lastDo)
}

// Conn represents a connection to a Uhaha Cluster
type Conn struct {
	released bool
	cl       *Cluster
	lastDo   time.Time
	conn     redis.Conn
}

// Release a connection back to the Cluster pool. This may close the network
// connection if the Cluster is released or there is no more room in the pool.
func (c *Conn) Release() {
	if c.released {
		return
	}
	c.released = true
	c.cl.mu.Lock()
	defer c.cl.mu.Unlock()
	if c.cl.released || c.cl.pool.Len() > c.cl.opts.PoolSize {
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
	} else {
		c.cl.pool.ReplaceOrInsert((*connItem)(c))
	}
}

// Do exectes a Uhaha command on the server and returns the reply or an error.
func (c *Conn) Do(commandName string, args ...interface{}) (reply interface{},
	err error,
) {
	if c.released {
		return nil, ErrConnectionReleased
	}
	var tryLeader string
	var requireLeader bool
	leaderStart := time.Now()
retryCommand:
	if time.Since(leaderStart) > c.cl.opts.LeadershipTimeout {
		return nil, ErrLeadershipTimeout
	}
	if c.conn == nil {
		connectionStart := time.Now()
	tryNewServer:
		if time.Since(connectionStart) > c.cl.opts.ConnectionTimeout {
			return nil, ErrConnectionTimeout
		}
		// open a new network connection
		addr, auth, tlscfg := c.cl.getConnectionInfo(false)
		if addr == "" {
			return nil, errors.New("no addresses defined")
		}
		if tryLeader != "" {
			addr, tryLeader = tryLeader, ""
		}
		conn, _, err := rawDial(addr, auth, tlscfg, requireLeader,
			c.cl.opts.ConnectionTimeout)
		if err != nil {
			if err.Error() == "ERR node is not the leader" {
				// requires a leader, try again
				time.Sleep(defaultRetryTimeout)
				goto retryCommand
			}
			if isNetworkError(err) {
				// try a new server
				time.Sleep(defaultRetryTimeout)
				goto tryNewServer
			}
			return nil, err
		}
		c.conn = conn
	}
	reply, err = c.conn.Do(commandName, args...)
	if err != nil {
		errMsg := err.Error()
		notTheLeader := errMsg == "ERR node is not the leader"
		if strings.HasPrefix(errMsg, "TRY ") {
			notTheLeader = true
			tryLeader = errMsg[4:]
		}
		if isNetworkError(err) || notTheLeader {
			// reset the connection and try a new server
			c.conn.Close()
			c.conn = nil
			if notTheLeader {
				requireLeader = true
			}
			time.Sleep(defaultRetryTimeout)
			goto retryCommand
		}
		return nil, err
	}
	c.lastDo = time.Now()
	return reply, nil
}

func isNetworkError(err error) bool {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	if _, ok := err.(net.Error); ok {
		return true
	}
	return false
}

func rawDial(addr, auth string, tlscfg *tls.Config, requireLeader bool,
	connTimeout time.Duration) (conn redis.Conn, servers []string, err error,
) {
	var opts []redis.DialOption
	if tlscfg != nil {
		opts = append(opts, redis.DialTLSConfig(tlscfg.Clone()))
	}
	opts = append(opts, redis.DialConnectTimeout(connTimeout))
	conn, err = redis.Dial("tcp", addr, opts...)
	if err != nil {
		return nil, nil, err
	}
	// do some handshaking stuff
	if err := func() (err error) {
		// talk to the server
		pong, err := redis.String(conn.Do("PING"))
		if err != nil {
			return err
		}
		if pong != "PONG" {
			return errors.New("expected 'PONG'")
		}
		// authenticate
		if auth != "" {
			ok, err := redis.String(conn.Do("AUTH", auth))
			if err != nil {
				return err
			}
			if ok != "OK" {
				return errors.New("expected 'OK'")
			}
		}
		// get server list
		vv, err := redis.Values(conn.Do("raft", "server", "list"))
		if err != nil {
			return err
		}
		if len(vv) == 0 {
			return errors.New("no servers found")
		}
		for _, v := range vv {
			m, err := redis.StringMap(v, nil)
			if err != nil {
				return err
			}
			servers = append(servers, m["address"])
		}
		if requireLeader {
			m, err := redis.StringMap(conn.Do("raft", "info", "state"))
			if err != nil {
				return err
			}
			if m["state"] != "Leader" {
				return errors.New("ERR node is not the leader")
			}
		}
		return nil
	}(); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, servers, nil
}

// ErrNil indicates that a reply value is nil.
var ErrNil = redis.ErrNil

// Int is a helper that converts a command reply to an integer. If err is not
// equal to nil, then Int returns 0, err. Otherwise, Int converts the
// reply to an int as follows:
//
//  Reply type    Result
//  integer       int(reply), nil
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Int(reply interface{}, err error) (int, error) {
	return redis.Int(reply, err)
}

// Int64 is a helper that converts a command reply to 64 bit integer. If err is
// not equal to nil, then Int64 returns 0, err. Otherwise, Int64 converts the
// reply to an int64 as follows:
//
//  Reply type    Result
//  integer       reply, nil
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Int64(reply interface{}, err error) (int64, error) {
	return redis.Int64(reply, err)
}

// Uint64 is a helper that converts a command reply to 64 bit unsigned integer.
// If err is not equal to nil, then Uint64 returns 0, err. Otherwise, Uint64
// converts the reply to an uint64 as follows:
//
//  Reply type    Result
//  +integer      reply, nil
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Uint64(reply interface{}, err error) (uint64, error) {
	return redis.Uint64(reply, err)
}

// Float64 is a helper that converts a command reply to 64 bit float. If err is
// not equal to nil, then Float64 returns 0, err. Otherwise, Float64 converts
// the reply to an int as follows:
//
//  Reply type    Result
//  bulk string   parsed reply, nil
//  nil           0, ErrNil
//  other         0, error
func Float64(reply interface{}, err error) (float64, error) {
	return redis.Float64(reply, err)
}

// String is a helper that converts a command reply to a string. If err is not
// equal to nil, then String returns "", err. Otherwise String converts the
// reply to a string as follows:
//
//  Reply type      Result
//  bulk string     string(reply), nil
//  simple string   reply, nil
//  nil             "",  ErrNil
//  other           "",  error
func String(reply interface{}, err error) (string, error) {
	return redis.String(reply, err)
}

// Bytes is a helper that converts a command reply to a slice of bytes. If err
// is not equal to nil, then Bytes returns nil, err. Otherwise Bytes converts
// the reply to a slice of bytes as follows:
//
//  Reply type      Result
//  bulk string     reply, nil
//  simple string   []byte(reply), nil
//  nil             nil, ErrNil
//  other           nil, error
func Bytes(reply interface{}, err error) ([]byte, error) {
	return redis.Bytes(reply, err)
}

// Bool is a helper that converts a command reply to a boolean. If err is not
// equal to nil, then Bool returns false, err. Otherwise Bool converts the
// reply to boolean as follows:
//
//  Reply type      Result
//  integer         value != 0, nil
//  bulk string     strconv.ParseBool(reply)
//  nil             false, ErrNil
//  other           false, error
func Bool(reply interface{}, err error) (bool, error) {
	return redis.Bool(reply, err)
}

// Values is a helper that converts an array command reply to a []interface{}.
// If err is not equal to nil, then Values returns nil, err. Otherwise, Values
// converts the reply as follows:
//
//  Reply type      Result
//  array           reply, nil
//  nil             nil, ErrNil
//  other           nil, error
func Values(reply interface{}, err error) ([]interface{}, error) {
	return redis.Values(reply, err)
}

// Float64s is a helper that converts an array command reply to a []float64. If
// err is not equal to nil, then Float64s returns nil, err. Nil array items are
// converted to 0 in the output slice. Floats64 returns an error if an array
// item is not a bulk string or nil.
func Float64s(reply interface{}, err error) ([]float64, error) {
	return redis.Float64s(reply, err)
}

// Strings is a helper that converts an array command reply to a []string. If
// err is not equal to nil, then Strings returns nil, err. Nil array items are
// converted to "" in the output slice. Strings returns an error if an array
// item is not a bulk string or nil.
func Strings(reply interface{}, err error) ([]string, error) {
	return redis.Strings(reply, err)
}

// ByteSlices is a helper that converts an array command reply to a [][]byte.
// If err is not equal to nil, then ByteSlices returns nil, err. Nil array
// items are stay nil. ByteSlices returns an error if an array item is not a
// bulk string or nil.
func ByteSlices(reply interface{}, err error) ([][]byte, error) {
	return redis.ByteSlices(reply, err)
}

// Int64s is a helper that converts an array command reply to a []int64.
// If err is not equal to nil, then Int64s returns nil, err. Nil array
// items are stay nil. Int64s returns an error if an array item is not a
// bulk string or nil.
func Int64s(reply interface{}, err error) ([]int64, error) {
	return redis.Int64s(reply, err)
}

// Ints is a helper that converts an array command reply to a []in.
// If err is not equal to nil, then Ints returns nil, err. Nil array
// items are stay nil. Ints returns an error if an array item is not a
// bulk string or nil.
func Ints(reply interface{}, err error) ([]int, error) {
	return redis.Ints(reply, err)
}

// StringMap is a helper that converts an array of strings (alternating key,
// value) into a map[string]string.
// Requires an even number of values in result.
func StringMap(reply interface{}, err error) (map[string]string, error) {
	return redis.StringMap(reply, err)
}

// IntMap is a helper that converts an array of strings (alternating key, value)
// into a map[string]int.
func IntMap(reply interface{}, err error) (map[string]int, error) {
	return redis.IntMap(reply, err)
}

// Int64Map is a helper that converts an array of strings (alternating key,
// value) into a map[string]int64. The HGETALL commands return replies in this
// format.
// Requires an even number of values in result.
func Int64Map(reply interface{}, err error) (map[string]int64, error) {
	return redis.Int64Map(reply, err)
}

// Uint64s is a helper that converts an array command reply to a []uint64.
// If err is not equal to nil, then Uint64s returns nil, err. Nil array
// items are stay nil. Uint64s returns an error if an array item is not a
// bulk string or nil.
func Uint64s(reply interface{}, err error) ([]uint64, error) {
	return redis.Uint64s(reply, err)
}

// Uint64Map is a helper that converts an array of strings (alternating key,
// value) into a map[string]uint64.
// Requires an even number of values in result.
func Uint64Map(reply interface{}, err error) (map[string]uint64, error) {
	return redis.Uint64Map(reply, err)
}
