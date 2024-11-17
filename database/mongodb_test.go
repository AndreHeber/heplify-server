package database

import (
	"testing"
	"time"

	"github.com/sipcapture/heplify-server/decoder"
)

func TestMongoDBSetup(t *testing.T) {
    m := &MongoDB{}
    err := m.setup()
    if err != nil {
        t.Fatalf("MongoDB setup failed: %v", err)
    }
}

func TestMongoDBInsert(t *testing.T) {
    m := &MongoDB{}
    err := m.setup()
    if err != nil {
        t.Fatalf("MongoDB setup failed: %v", err)
    }

    hCh := make(chan *decoder.HEP, 1)
    hCh <- &decoder.HEP{
        SID:       "test_sid",
        Timestamp: time.Now(),
        ProtoType: 1,
        Payload:   "test_payload",
        Raw:       []byte("test_raw"),
    }
    close(hCh)

    m.insert(hCh)
}