package tests

import (
	"strings"
	"testing"

	"github.com/sartoopjj/vpn-over-github/shared"
)

func sampleBatch(frameCount int, payloadSize int) *shared.Batch {
	frames := make([]shared.Frame, frameCount)
	for i := range frames {
		frames[i] = shared.Frame{
			ConnID: "conn_abcdef0123456789",
			Seq:    int64(i + 1),
			Dst:    "example.com:443",
			Data:   strings.Repeat("A", payloadSize),
			Status: shared.FrameActive,
		}
	}
	return &shared.Batch{Epoch: 12345, Seq: 42, Ts: 1700000000, Frames: frames}
}

func TestWire_BytesRoundtripSmall(t *testing.T) {
	b := sampleBatch(1, 16)
	encoded, err := shared.EncodeBatchBytes(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := shared.DecodeBatchBytes(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Epoch != b.Epoch || decoded.Seq != b.Seq || len(decoded.Frames) != 1 {
		t.Fatalf("roundtrip mismatch: %+v", decoded)
	}
	if decoded.Frames[0].Data != b.Frames[0].Data {
		t.Fatalf("data mismatch")
	}
}

func TestWire_BytesRoundtripLargeAndCompresses(t *testing.T) {
	// 50 frames × 200 bytes of repeated data = highly compressible JSON.
	b := sampleBatch(50, 200)
	encoded, err := shared.EncodeBatchBytes(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// gzip magic should be present for a payload this compressible.
	if len(encoded) < 2 || !(encoded[0] == 0x1F && encoded[1] == 0x8B) {
		t.Fatalf("expected gzip header, got %x", encoded[:min(8, len(encoded))])
	}

	decoded, err := shared.DecodeBatchBytes(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Frames) != 50 {
		t.Fatalf("expected 50 frames, got %d", len(decoded.Frames))
	}
}

func TestWire_StringRoundtripSmall(t *testing.T) {
	b := sampleBatch(1, 16)
	encoded, err := shared.EncodeBatchString(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.HasPrefix(encoded, "{") {
		t.Fatalf("expected plain JSON for small payload, got %q", encoded[:min(20, len(encoded))])
	}
	decoded, err := shared.DecodeBatchString(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Epoch != b.Epoch || len(decoded.Frames) != 1 {
		t.Fatalf("roundtrip mismatch: %+v", decoded)
	}
}

func TestWire_StringRoundtripLargeUsesGzipPrefix(t *testing.T) {
	b := sampleBatch(50, 200)
	encoded, err := shared.EncodeBatchString(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !strings.HasPrefix(encoded, "G1:") {
		t.Fatalf("expected G1: prefix for compressible payload, got %q", encoded[:min(10, len(encoded))])
	}
	decoded, err := shared.DecodeBatchString(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Frames) != 50 {
		t.Fatalf("expected 50 frames, got %d", len(decoded.Frames))
	}
}

func TestWire_DecodeBackCompatPlainJSON(t *testing.T) {
	// A reader must accept legacy plain-JSON Batch bytes.
	plain := []byte(`{"epoch":7,"seq":3,"ts":0,"frames":[]}`)
	decoded, err := shared.DecodeBatchBytes(plain)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Epoch != 7 || decoded.Seq != 3 {
		t.Fatalf("unexpected decoded batch: %+v", decoded)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
