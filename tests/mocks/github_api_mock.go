package mocks

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

// MockTransport is an in-memory implementation of shared.Transport.
type MockTransport struct {
	mu       sync.Mutex
	nextID   int
	channels map[string]*mockChannel
}

type mockChannel struct {
	id          string
	description string
	updatedAt   time.Time
	batches     map[string]*shared.Batch
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		channels: make(map[string]*mockChannel),
	}
}

func (m *MockTransport) EnsureChannel(_ context.Context, existingID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingID != "" {
		if _, ok := m.channels[existingID]; ok {
			return existingID, nil
		}
	}

	m.nextID++
	id := fmt.Sprintf("ch-%d", m.nextID)
	m.channels[id] = &mockChannel{
		id:          id,
		description: shared.ChannelDescPrefix + id,
		updatedAt:   time.Now(),
		batches: map[string]*shared.Batch{
			shared.ClientBatchFile: {Seq: 0, Ts: 0, Frames: nil},
			shared.ServerBatchFile: {Seq: 0, Ts: 0, Frames: nil},
		},
	}
	return id, nil
}

func (m *MockTransport) DeleteChannel(_ context.Context, channelID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.channels, channelID)
	return nil
}

func (m *MockTransport) ListChannels(_ context.Context) ([]*shared.ChannelInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]*shared.ChannelInfo, 0, len(m.channels))
	for _, ch := range m.channels {
		out = append(out, &shared.ChannelInfo{ID: ch.id, Description: ch.description, UpdatedAt: ch.updatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *MockTransport) Write(_ context.Context, channelID, filename string, batch *shared.Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.channels[channelID]
	if !ok {
		return shared.ErrNotFound
	}
	copied := &shared.Batch{
		Epoch:  batch.Epoch,
		Seq:    batch.Seq,
		Ts:     batch.Ts,
		Frames: append([]shared.Frame(nil), batch.Frames...),
	}
	ch.batches[filename] = copied
	ch.updatedAt = time.Now()
	return nil
}

func (m *MockTransport) Read(_ context.Context, channelID, filename string) (*shared.Batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.channels[channelID]
	if !ok {
		return nil, shared.ErrNotFound
	}
	batch, ok := ch.batches[filename]
	if !ok {
		return nil, nil
	}
	if batch == nil {
		return nil, nil
	}
	copied := &shared.Batch{
		Epoch:  batch.Epoch,
		Seq:    batch.Seq,
		Ts:     batch.Ts,
		Frames: append([]shared.Frame(nil), batch.Frames...),
	}
	return copied, nil
}

func (m *MockTransport) GetRateLimitInfo() shared.RateLimitInfo {
	return shared.RateLimitInfo{Remaining: 5000, Limit: 5000, Resource: "core", LastUpdated: time.Now()}
}

func (m *MockTransport) SetUpdatedAt(channelID string, ts time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch, ok := m.channels[channelID]
	if !ok {
		return shared.ErrNotFound
	}
	ch.updatedAt = ts
	return nil
}

var _ shared.Transport = (*MockTransport)(nil)
