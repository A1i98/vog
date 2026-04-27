package shared

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
)

// gzipPrefix marks a string-encoded compressed batch. Plain JSON starts with
// '{' so the two are unambiguous.
const gzipPrefix = "G1:"

// compressMinSize: don't gzip below this raw size — overhead outweighs gain.
const compressMinSize = 256

// EncodeBatchBytes returns binary wire bytes for a Batch, gzip-compressed
// when worthwhile. Used by binary-safe transports (git).
func EncodeBatchBytes(b *Batch) ([]byte, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("marshal batch: %w", err)
	}
	if len(raw) < compressMinSize {
		return raw, nil
	}
	compressed, err := gzipBytes(raw)
	if err != nil || len(compressed) >= len(raw) {
		return raw, nil
	}
	return compressed, nil
}

// DecodeBatchBytes parses raw JSON or gzip-of-JSON into a Batch.
func DecodeBatchBytes(data []byte) (*Batch, error) {
	raw := data
	if len(data) >= 2 && data[0] == 0x1F && data[1] == 0x8B {
		decompressed, err := gunzipBytes(data)
		if err != nil {
			return nil, fmt.Errorf("gunzip: %w", err)
		}
		raw = decompressed
	}
	var b Batch
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("unmarshal batch: %w", err)
	}
	return &b, nil
}

// EncodeBatchString returns a wire string for a Batch. Compressed form is
// gzipPrefix + base64(gzip(json)); uncompressed form is plain JSON. Used by
// text-only transports (gist).
func EncodeBatchString(b *Batch) (string, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return "", fmt.Errorf("marshal batch: %w", err)
	}
	if len(raw) < compressMinSize {
		return string(raw), nil
	}
	compressed, err := gzipBytes(raw)
	if err != nil {
		return string(raw), nil
	}
	encoded := base64.StdEncoding.EncodeToString(compressed)
	if len(gzipPrefix)+len(encoded) >= len(raw) {
		return string(raw), nil
	}
	return gzipPrefix + encoded, nil
}

// DecodeBatchString parses a wire string (plain JSON or gzipPrefix+base64)
// into a Batch.
func DecodeBatchString(s string) (*Batch, error) {
	if len(s) > len(gzipPrefix) && s[:len(gzipPrefix)] == gzipPrefix {
		compressed, err := base64.StdEncoding.DecodeString(s[len(gzipPrefix):])
		if err != nil {
			return nil, fmt.Errorf("base64 decode: %w", err)
		}
		return DecodeBatchBytes(compressed)
	}
	return DecodeBatchBytes([]byte(s))
}

func gzipBytes(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz, err := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := gz.Write(in); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzipBytes(in []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	return io.ReadAll(gz)
}
