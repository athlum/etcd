package wal

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
)

func TestBatchWriteEntry(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := Create(p, []byte("somedata"))
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	l := 1000
	size := 1000
	ents := make([]*raftpb.Entry, l)
	for i := 0; i < l; i += 1 {
		data := make([]byte, size)
		for i := 0; i < size; i++ {
			data[i] = byte(i)
		}
		ents[i] = &raftpb.Entry{Data: data}
	}

	batch := w.encoder.batch()
	rc, stop := batch.start(len(ents) + 1)
	// TODO(xiangli): no more reference operator
	for i := range ents {
		w.saveEntryC(ents[i], rc)
	}

	err = <-batch.ec
	stop()
	if err != io.EOF {
		t.Fatal(err)
	}
}
