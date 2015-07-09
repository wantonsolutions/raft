package raft

import (
	"reflect"
	"testing"
	"time"
)

const nodes = 5

func TestThreeWay_AppendEntries(t *testing.T) {
	cluster := make([]NetworkTransport, nodes)
	rpcChans := make([]<-chan RPC, nodes)
	for i := range cluster {
		host, err := NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, nil)
		if err != nil {
			t.Fatalf("err: %v", err)
		} else {
			cluster[i] = *host
		}
		rpcChans[i] = cluster[i].Consumer()
	}

	// Make the RPC request
	args := AppendEntriesRequest{
		Term:         10,
		Leader:       []byte("cartman"),
		PrevLogEntry: 100,
		PrevLogTerm:  4,
		Entries: []*Log{
			&Log{
				Index: 101,
				Term:  4,
				Type:  LogNoop,
			},
		},
		LeaderCommitIndex: 90,
	}
	resp := AppendEntriesResponse{
		Term:    4,
		LastLog: 90,
		Success: true,
	}

	listen := func(i int) {
		select {
		case rpc := <-rpcChans[i]:
			// Verify the command
			req := rpc.Command.(*AppendEntriesRequest)
			if !reflect.DeepEqual(req, &args) {
				t.Fatalf("command mismatch: %#v %#v", *req, args)
			}

			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			t.Fatalf("timeout")
		}
	}
	for i := 1; i < nodes; i++ {
		go listen(i)
	}

	for i := 1; i < nodes; i++ {
		var out AppendEntriesResponse
		if err := cluster[0].AppendEntries(cluster[i].LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}
	}

}

func clusterClose(c []NetworkTransport) {
	for i := range c {
		c[i].Close()
	}
}
