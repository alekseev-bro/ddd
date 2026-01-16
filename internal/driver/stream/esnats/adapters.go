package esnats

import (
	"log/slog"
	"strings"
	"time"

	"github.com/alekseev-bro/ddd/pkg/aggregate"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsMessage interface {
	Headers() nats.Header
	Data() []byte
	Subject() string
	Seq() uint64
	Timestamp() time.Time
}

type jsRawMsgAdapter struct {
	*jetstream.RawStreamMsg
}

func (j jsRawMsgAdapter) Headers() nats.Header {
	return j.RawStreamMsg.Header
}
func (j jsRawMsgAdapter) Timestamp() time.Time {
	return j.RawStreamMsg.Time
}

func (j jsRawMsgAdapter) Data() []byte {
	return j.RawStreamMsg.Data
}

func (j jsRawMsgAdapter) Subject() string {

	return j.RawStreamMsg.Subject
}

func (j jsRawMsgAdapter) Seq() uint64 {
	return j.RawStreamMsg.Sequence
}

type natsMessageAdapter struct {
	*nats.Msg
}

func (n natsMessageAdapter) Headers() nats.Header {
	return n.Msg.Header
}

// TODO: check panic for NATS core
func (n natsMessageAdapter) Timestamp() time.Time {
	mt, err := n.Msg.Metadata()
	if err != nil {
		slog.Error("failed to get metadata", "error", err)
		panic("failed to get metadata")
	}
	return mt.Timestamp
}

func (n natsMessageAdapter) Data() []byte {
	return n.Msg.Data
}

func (n natsMessageAdapter) Subject() string {

	return n.Msg.Subject
}

// TODO: check panic for NATS core
func (n natsMessageAdapter) Seq() uint64 {
	mt, err := n.Msg.Metadata()
	if err != nil {
		slog.Error("failed to get metadata", "error", err)
		panic("failed to get metadata")
	}
	return mt.Sequence.Stream
}

type natsJSMsgAdapter struct {
	jetstream.Msg
}

func (n natsJSMsgAdapter) Timestamp() time.Time {
	mt, err := n.Msg.Metadata()
	if err != nil {
		slog.Error("failed to get metadata", "error", err)
		panic("failed to get metadata")
	}
	return mt.Timestamp
}

func (n natsJSMsgAdapter) Seq() uint64 {
	mt, err := n.Msg.Metadata()
	if err != nil {
		slog.Error("failed to get metadata", "error", err)
		panic("failed to get metadata")
	}
	return mt.Sequence.Stream
}

func eventFromMsg(msg natsMessage) *aggregate.StoredMsg {

	mid, err := uuid.Parse(msg.Headers().Get(jetstream.MsgIDHeader))
	if err != nil {
		slog.Error("failed to parse uuid", "error", err)
		panic("failed to parse uuid")
	}
	subjectParts := strings.Split(msg.Subject(), ".")
	kind := subjectParts[2]

	//	ev := typereg.GetType(kind, msg.Data())

	return &aggregate.StoredMsg{
		Msg: aggregate.Msg{
			ID:   aggregate.ID(mid),
			Kind: kind,
			Body: msg.Data(),
		},
		Version: aggregate.Version{Sequence: msg.Seq(), Timestamp: msg.Timestamp()},
	}
}
