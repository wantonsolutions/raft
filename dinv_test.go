package raft

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
)

func TestRaft_TripleNode(t *testing.T) {
	fmt.Printf("STARTING RAFT TRIPLE NODE TEST\n")
	// Make the cluster
	c := MakeCluster(3, t, nil)
	defer c.Close()

	// Should be one leader
	leader := c.Leader()
	c.EnsureLeader(t, leader.localAddr)

	// Should be able to apply
	future := leader.Apply([]byte("test"), time.Millisecond)
	if err := future.Error(); err != nil {
		t.Fatalf("err: %v", err)
	}

	// Wait for replication
	time.Sleep(30 * time.Millisecond)

	// Check that it is applied to the FSM
	for _, fsm := range c.fsms {
		fsm.Lock()
		num := len(fsm.logs)
		fsm.Unlock()
		if num != 1 {
			t.Fatalf("did not apply to FSM!")
		}
	}
	fmt.Printf("ENDING RAFT TRIPLE NODE TEST\n")
}

/*-----------------------------------------------------------------*/
/*			Functions pulled from raft_test.go					   */
/*-----------------------------------------------------------------*/

/*-----------------------------------------------------------------*/
/*			MockFSM functions					   */
/*-----------------------------------------------------------------*/
// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially
type MockFSM struct {
	sync.Mutex
	logs [][]byte
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

func (m *MockFSM) Apply(log *Log) interface{} {
	m.Lock()
	defer m.Unlock()
	m.logs = append(m.logs, log.Data)
	return len(m.logs)
}

func (m *MockFSM) Snapshot() (FSMSnapshot, error) {
	m.Lock()
	defer m.Unlock()
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer inp.Close()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(inp, &hd)

	m.logs = nil
	return dec.Decode(&m.logs)
}

func (m *MockSnapshot) Persist(sink SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	if err := enc.Encode(m.logs[:m.maxIndex]); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

func (m *MockSnapshot) Release() {
}

// Return configurations optimized for in-memory
func inmemConfig() *Config {
	conf := DefaultConfig()
	conf.HeartbeatTimeout = 50 * time.Millisecond
	conf.ElectionTimeout = 50 * time.Millisecond
	conf.LeaderLeaseTimeout = 50 * time.Millisecond
	conf.CommitTimeout = time.Millisecond
	return conf
}

/*-----------------------------------------------------------------*/
/*			ClusterFunctions									   */
/*-----------------------------------------------------------------*/
type cluster struct {
	dirs   []string
	stores []*InmemStore
	fsms   []*MockFSM
	snaps  []*FileSnapshotStore
	trans  []*InmemTransport
	rafts  []*Raft
}

func (c *cluster) Merge(other *cluster) {
	c.dirs = append(c.dirs, other.dirs...)
	c.stores = append(c.stores, other.stores...)
	c.fsms = append(c.fsms, other.fsms...)
	c.snaps = append(c.snaps, other.snaps...)
	c.trans = append(c.trans, other.trans...)
	c.rafts = append(c.rafts, other.rafts...)
}

func (c *cluster) Close() {
	var futures []Future
	for _, r := range c.rafts {
		futures = append(futures, r.Shutdown())
	}

	// Wait for shutdown
	timer := time.AfterFunc(200*time.Millisecond, func() {
		panic("timed out waiting for shutdown")
	})

	for _, f := range futures {
		if err := f.Error(); err != nil {
			panic(fmt.Errorf("shutdown future err: %v", err))
		}
	}
	timer.Stop()

	for _, d := range c.dirs {
		os.RemoveAll(d)
	}
}

func (c *cluster) GetInState(s RaftState) []*Raft {
	in := make([]*Raft, 0, 1)
	for _, r := range c.rafts {
		if r.State() == s {
			in = append(in, r)
		}
	}
	return in
}

func (c *cluster) Leader() *Raft {
	timeout := time.AfterFunc(400*time.Millisecond, func() {
		panic("timeout waiting for leader")
	})
	defer timeout.Stop()

	for len(c.GetInState(Leader)) < 1 {
		time.Sleep(time.Millisecond)
	}
	leaders := c.GetInState(Leader)
	if len(leaders) != 1 {
		panic(fmt.Errorf("expected one leader: %v", leaders))
	}
	return leaders[0]
}

func (c *cluster) FullyConnect() {
	log.Printf("[WARN] Fully Connecting")
	for i, t1 := range c.trans {
		for j, t2 := range c.trans {
			if i != j {
				t1.Connect(t2.LocalAddr(), t2)
				t2.Connect(t1.LocalAddr(), t1)
			}
		}
	}
}

func (c *cluster) Disconnect(a string) {
	log.Printf("[WARN] Disconnecting %v", a)
	for _, t := range c.trans {
		if t.localAddr == a {
			t.DisconnectAll()
		} else {
			t.Disconnect(a)
		}
	}
}

func (c *cluster) EnsureLeader(t *testing.T, expect string) {
	limit := time.Now().Add(400 * time.Millisecond)
CHECK:
	for _, r := range c.rafts {
		leader := r.Leader()
		if expect == "" {
			if leader != "" {
				if time.Now().After(limit) {
					t.Fatalf("leader %v expected nil", leader)
				} else {
					goto WAIT
				}
			}
		} else {
			if leader == "" || leader != expect {
				if time.Now().After(limit) {
					t.Fatalf("leader %v expected %v", leader, expect)
				} else {
					goto WAIT
				}
			}
		}
	}

	return
WAIT:
	time.Sleep(10 * time.Millisecond)
	goto CHECK
}

func (c *cluster) EnsureSame(t *testing.T) {
	limit := time.Now().Add(400 * time.Millisecond)
	first := c.fsms[0]

CHECK:
	first.Lock()
	for i, fsm := range c.fsms {
		if i == 0 {
			continue
		}
		fsm.Lock()

		if len(first.logs) != len(fsm.logs) {
			fsm.Unlock()
			if time.Now().After(limit) {
				t.Fatalf("length mismatch: %d %d",
					len(first.logs), len(fsm.logs))
			} else {
				goto WAIT
			}
		}

		for idx := 0; idx < len(first.logs); idx++ {
			if bytes.Compare(first.logs[idx], fsm.logs[idx]) != 0 {
				fsm.Unlock()
				if time.Now().After(limit) {
					t.Fatalf("log mismatch at index %d", idx)
				} else {
					goto WAIT
				}
			}
		}
		fsm.Unlock()
	}

	first.Unlock()
	return

WAIT:
	first.Unlock()
	time.Sleep(20 * time.Millisecond)
	goto CHECK
}

func MakeCluster(n int, t *testing.T, conf *Config) *cluster {
	c := &cluster{}
	peers := make([]string, 0, n)

	// Setup the stores and transports
	for i := 0; i < n; i++ {
		dir, err := ioutil.TempDir("", "raft")
		if err != nil {
			t.Fatalf("err: %v ", err)
		}
		store := NewInmemStore()
		c.dirs = append(c.dirs, dir)
		c.stores = append(c.stores, store)
		c.fsms = append(c.fsms, &MockFSM{})

		dir2, snap := FileSnapTest(t)
		c.dirs = append(c.dirs, dir2)
		c.snaps = append(c.snaps, snap)

		addr, trans := NewInmemTransport()
		c.trans = append(c.trans, trans)
		peers = append(peers, addr)
	}

	// Wire the transports together
	c.FullyConnect()

	// Create all the rafts
	for i := 0; i < n; i++ {
		if conf == nil {
			conf = inmemConfig()
		}
		if n == 1 {
			conf.EnableSingleNode = true
		}

		logs := c.stores[i]
		store := c.stores[i]
		snap := c.snaps[i]
		trans := c.trans[i]
		peerStore := &StaticPeers{StaticPeers: peers}

		raft, err := NewRaft(conf, c.fsms[i], logs, store, snap, peerStore, trans)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		c.rafts = append(c.rafts, raft)
	}

	return c
}

func FileSnapTest(t *testing.T) (string, *FileSnapshotStore) {
	// Create a test dir
	dir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("err: %v ", err)
	}

	snap, err := NewFileSnapshotStore(dir, 3, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return dir, snap
}
