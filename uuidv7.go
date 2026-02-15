package gqm

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// NewUUID generates a UUID v7 (RFC 9562) string.
// Format: xxxxxxxx-xxxx-7xxx-yxxx-xxxxxxxxxxxx
// 48-bit millisecond timestamp + 4-bit version (0111) + 12-bit random +
// 2-bit variant (10) + 62-bit random.
func NewUUID() string {
	return newUUIDAt(time.Now())
}

var (
	uuidMu   sync.Mutex
	lastMS   int64
	uuidSeq  uint16
)

func newUUIDAt(t time.Time) string {
	ms := t.UnixMilli()

	uuidMu.Lock()
	if ms == lastMS {
		uuidSeq++
	} else {
		lastMS = ms
		uuidSeq = 0
	}
	seq := uuidSeq
	uuidMu.Unlock()

	var uuid [16]byte

	// Bytes 0-5: 48-bit big-endian millisecond timestamp
	binary.BigEndian.PutUint16(uuid[0:2], uint16(ms>>32))
	binary.BigEndian.PutUint32(uuid[2:6], uint32(ms))

	// Bytes 6-7: version (0111) + 12-bit seq_hi (from seq + random)
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])

	// Use seq in lower 12 bits of bytes 6-7 for sub-millisecond ordering
	uuid[6] = 0x70 | byte(seq>>8)&0x0F
	uuid[7] = byte(seq)

	// Bytes 8-15: variant (10) + 62-bit random
	uuid[8] = 0x80 | (rnd[0] & 0x3F)
	copy(uuid[9:], rnd[1:8])

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(uuid[0:4]),
		binary.BigEndian.Uint16(uuid[4:6]),
		binary.BigEndian.Uint16(uuid[6:8]),
		binary.BigEndian.Uint16(uuid[8:10]),
		uuid[10:16],
	)
}
