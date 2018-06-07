package wal

import (
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal/walpb"
	"io"
)

func (e *encoder) crcSum(data []byte) uint32 {
	e.crc.Write(data)
	return e.crc.Sum32()
}

func (w *WAL) saveEntryC(e *raftpb.Entry, rc chan *walpb.Record) {
	// TODO: add MustMarshalTo to reduce one allocation.
	b := pbutil.MustMarshal(e)
	rc <- &walpb.Record{Type: entryType, Data: b}
	w.enti = e.Index
}

func (w *WAL) saveStateC(s *raftpb.HardState, rc chan *walpb.Record) {
	if raft.IsEmptyHardState(*s) {
		return
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rc <- &walpb.Record{Type: stateType, Data: b}
}

type batchEncoder struct {
	*encoder
	ec chan error
}

type batchMsg struct {
	data     []byte
	lenField uint64
	padBytes int
}

func (e *encoder) batch() *batchEncoder {
	return &batchEncoder{
		encoder: e,
		ec:      make(chan error),
	}
}

func (e *batchEncoder) worker(mc chan batchMsg, stopc chan struct{}, l int) {
	for i := 0; i < l-1; i += 1 {
		select {
		case m := <-mc:
			if err := writeUint64(e.bw, m.lenField, e.uint64buf); err != nil {
				e.ec <- err
				return
			}
			if m.padBytes != 0 {
				m.data = append(m.data, make([]byte, m.padBytes)...)
			}
			if _, err := e.bw.Write(m.data); err != nil {
				e.ec <- err
				return
			}
		case <-stopc:
			return
		}
	}
	e.ec <- io.EOF
}

func (e *batchEncoder) marshalc(rc chan *walpb.Record, mc chan batchMsg, stopc chan struct{}) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		data []byte
		err  error
		n    int
		buf  = make([]byte, 10240*1024)
	)

	for {
		select {
		case r := <-rc:
			r.Crc = e.crcSum(r.Data)
			if r.Size() > len(buf) {
				data, err = r.Marshal()
				if err != nil {
					e.ec <- err
					return
				}
			} else {
				n, err = r.MarshalTo(buf)
				if err != nil {
					e.ec <- err
					return
				}
				data = buf[:n]
			}
			lenField, padBytes := encodeFrameSize(len(data))
			mc <- batchMsg{data, lenField, padBytes}
		case <-stopc:
			return
		}
	}
}

func (e *batchEncoder) start(l int) (chan *walpb.Record, func()) {
	var (
		stopc = make(chan struct{})
		stop  = func() {
			close(stopc)
		}
		rc = make(chan *walpb.Record, l)
		mc = make(chan batchMsg, l)
	)

	go e.worker(mc, stopc, l)
	go e.marshalc(rc, mc, stopc)
	return rc, stop
}
