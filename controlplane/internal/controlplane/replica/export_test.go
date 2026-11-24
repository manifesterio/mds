package replica

import "github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"

// QueueDepth is the number of client ops waiting for a commit slot.
func (m *Replica) QueueDepth() int { return len(m.queue) }

// Booting reports restore or catch-up in progress (not yet Ready).
func (m *Replica) Booting() bool { return m.booting }

// DecodeGroup exports group decoding for tests (strips the envelope).
func DecodeGroup(data []byte) (string, []metadb.Entry, bool) {
	return decodeGroup(data)
}

// EncodeEnvelopedForTest wraps an inner payload with epoch and writer.
func EncodeEnvelopedForTest(epoch uint64, writer string, payload []byte) []byte {
	return wrap(epoch, writer, payload)
}
