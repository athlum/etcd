package etcdserver

import (
	"fmt"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"net/http"
	"time"
)

type routedTransporter struct {
	transporter     *rafthttp.Transport
	tranTransporter *rafthttp.Transport
}

func warppedTransport(t, tt *rafthttp.Transport) *routedTransporter {
	return &routedTransporter{
		transporter:     t,
		tranTransporter: tt,
	}
}

func (t *routedTransporter) Main() rafthttp.Transporter {
	return t.transporter
}

func (t *routedTransporter) Start() error {
	return fmt.Errorf("routedTransporter can't start.")
}

func (t *routedTransporter) Handler() http.Handler {
	return t.transporter.Handler()
}

func (t *routedTransporter) Send(m []raftpb.Message) {
	nm := []raftpb.Message{}
	sm := []raftpb.Message{}

	for _, msg := range m {
		if msg.Type == raftpb.MsgHeartbeat || msg.Type == raftpb.MsgHeartbeatResp {
			sm = append(sm, msg)
		} else {
			nm = append(nm, msg)
		}
	}
	if len(nm) > 0 {
		t.transporter.Send(nm)
	}
	if len(sm) > 0 {
		t.tranTransporter.Send(sm)
	}
}

func (t *routedTransporter) SendSnapshot(m snap.Message) {
	t.transporter.SendSnapshot(m)
}

func (t *routedTransporter) AddRemote(id types.ID, urls, tUrls []string) {
	t.transporter.AddRemote(id, urls)
	t.tranTransporter.AddRemote(id, tUrls)
}

func (t *routedTransporter) AddPeer(id types.ID, urls, tUrls []string) {
	t.transporter.AddPeer(id, urls)
	t.tranTransporter.AddPeer(id, tUrls)
}

func (t *routedTransporter) RemovePeer(id types.ID) {
	t.transporter.RemovePeer(id)
	t.tranTransporter.RemovePeer(id)
}

func (t *routedTransporter) CutPeer(id types.ID) {
	t.transporter.CutPeer(id)
	t.tranTransporter.CutPeer(id)
}

func (t *routedTransporter) MendPeer(id types.ID) {
	t.transporter.MendPeer(id)
	t.tranTransporter.MendPeer(id)
}

func (t *routedTransporter) RemoveAllPeers() {
	t.transporter.RemoveAllPeers()
	t.tranTransporter.RemoveAllPeers()
}

func (t *routedTransporter) Pause() {
	t.transporter.Pause()
	t.tranTransporter.Pause()
}

func (t *routedTransporter) Resume() {
	t.transporter.Resume()
	t.tranTransporter.Resume()
}

func (t *routedTransporter) UpdatePeer(id types.ID, urls, tUrls []string) {
	t.transporter.UpdatePeer(id, urls)
	t.tranTransporter.UpdatePeer(id, tUrls)
}

func (t *routedTransporter) ActiveSince(id types.ID) time.Time {
	return t.transporter.ActiveSince(id)
}

func (t *routedTransporter) ActivePeers() int {
	return t.transporter.ActivePeers()
}

func (t *routedTransporter) Stop() {
	t.transporter.Stop()
	t.tranTransporter.Stop()
}
