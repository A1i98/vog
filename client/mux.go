package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

// upstreamChannel is one pre-allocated channel (gist or git dir) with its
// own writer + reader goroutines. flushSig wakes the writer between ticks.
type upstreamChannel struct {
	channelID string
	tokenIdx  int
	transport shared.Transport
	encryptor *shared.Encryptor

	transportKind string
	batchInterval time.Duration
	fetchInterval time.Duration

	epoch    int64
	batchSeq atomic.Int64

	lastReadEpoch atomic.Int64
	lastReadSeq   atomic.Int64

	flushSig chan struct{}
}

func (ch *upstreamChannel) signalFlush() {
	select {
	case ch.flushSig <- struct{}{}:
	default:
	}
}

// VirtualConn is a single SOCKS connection multiplexed over an upstream channel.
type VirtualConn struct {
	connID    string
	dst       string
	channel   *upstreamChannel
	mux       *MuxClient
	recvBuf   chan []byte
	closed    chan struct{}
	closeOnce sync.Once

	openSent  atomic.Bool
	closeSent atomic.Bool

	mu         sync.Mutex
	writeQueue []writeChunk
	readBuf    []byte

	seq       atomic.Int64
	bytesUp   atomic.Int64
	bytesDown atomic.Int64
	startTime time.Time
}

type writeChunk struct {
	data []byte
	seq  int64
}

func (vc *VirtualConn) Write(p []byte) (int, error) {
	select {
	case <-vc.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	seq := vc.seq.Add(1)
	vc.mu.Lock()
	vc.writeQueue = append(vc.writeQueue, writeChunk{data: buf, seq: seq})
	vc.mu.Unlock()
	vc.bytesUp.Add(int64(len(p)))
	if vc.mux != nil {
		vc.mux.totalBytesUp.Add(int64(len(p)))
	}
	if vc.channel != nil {
		vc.channel.signalFlush()
	}
	return len(p), nil
}

func (vc *VirtualConn) Read(p []byte) (int, error) {
	vc.mu.Lock()
	if len(vc.readBuf) > 0 {
		n := copy(p, vc.readBuf)
		vc.readBuf = vc.readBuf[n:]
		vc.mu.Unlock()
		vc.bytesDown.Add(int64(n))
		if vc.mux != nil {
			vc.mux.totalBytesDown.Add(int64(n))
		}
		return n, nil
	}
	vc.mu.Unlock()

	select {
	case data, ok := <-vc.recvBuf:
		if !ok {
			return 0, io.EOF
		}
		n := copy(p, data)
		if n < len(data) {
			vc.mu.Lock()
			vc.readBuf = append(vc.readBuf[:0], data[n:]...)
			vc.mu.Unlock()
		}
		vc.bytesDown.Add(int64(n))
		if vc.mux != nil {
			vc.mux.totalBytesDown.Add(int64(n))
		}
		return n, nil
	case <-vc.closed:
		return 0, io.EOF
	}
}

func (vc *VirtualConn) Close() error {
	vc.closeOnce.Do(func() {
		close(vc.closed)
		if vc.channel != nil {
			vc.channel.signalFlush()
		}
	})
	return nil
}

// MuxClient multiplexes all SOCKS connections through one or more upstream
// channels (one per token, configurable count). Round-robin assigns each
// new VirtualConn to a channel.
type MuxClient struct {
	cfg         *Config
	rateLimiter *RateLimiter
	channels    []*upstreamChannel

	mu          sync.RWMutex
	conns       map[string]*VirtualConn
	nextChannel uint64

	// Lifetime counters: monotonically increasing across the whole client
	// run, including bytes from conns that have already been GC'd.
	totalBytesUp   atomic.Int64
	totalBytesDown atomic.Int64
}

// TotalBytes returns cumulative tunneled bytes since startup.
func (m *MuxClient) TotalBytes() (up, down int64) {
	return m.totalBytesUp.Load(), m.totalBytesDown.Load()
}

func NewMuxClient(ctx context.Context, cfg *Config, rl *RateLimiter, transports map[int]shared.Transport) (*MuxClient, error) {
	defaultN := cfg.GitHub.UpstreamConnections
	if defaultN <= 0 {
		defaultN = 2
	}

	m := &MuxClient{
		cfg:         cfg,
		rateLimiter: rl,
		conns:       make(map[string]*VirtualConn),
	}

	var wg sync.WaitGroup
	var initMu sync.Mutex
	var initErr error

	for tokenIdx, transport := range transports {
		wg.Add(1)
		go func(tokenIdx int, transport shared.Transport) {
			defer wg.Done()
			n := defaultN
			if tokenIdx < len(cfg.GitHub.Tokens) {
				n = cfg.GitHub.Tokens[tokenIdx].EffectiveUpstreamConnections(defaultN)
			}
			channels, err := m.initTokenChannels(ctx, tokenIdx, transport, n)
			initMu.Lock()
			defer initMu.Unlock()
			if err != nil {
				slog.Error("token channel initialization failed", "token_idx", tokenIdx, "error", err)
				if initErr == nil {
					initErr = err
				}
			}
			m.channels = append(m.channels, channels...)
		}(tokenIdx, transport)
	}

	wg.Wait()

	if len(m.channels) == 0 {
		if initErr != nil {
			return nil, fmt.Errorf("no upstream channels available: %w", initErr)
		}
		return nil, fmt.Errorf("no upstream channels available")
	}

	for _, ch := range m.channels {
		go m.batchWriteLoop(ctx, ch)
		go m.batchReadLoop(ctx, ch)
	}
	return m, nil
}

// Connect opens a new virtual connection to dst. The OPEN frame is sent on
// the next flush (signalled immediately).
func (m *MuxClient) Connect(_ context.Context, dst string) (*VirtualConn, error) {
	connID, err := shared.GenerateConnID()
	if err != nil {
		return nil, fmt.Errorf("generating conn ID: %w", err)
	}

	m.mu.Lock()
	idx := m.nextChannel % uint64(len(m.channels))
	m.nextChannel++
	ch := m.channels[idx]
	vc := &VirtualConn{
		connID:    connID,
		dst:       dst,
		channel:   ch,
		mux:       m,
		recvBuf:   make(chan []byte, 256),
		closed:    make(chan struct{}),
		startTime: time.Now(),
	}
	m.conns[connID] = vc
	m.mu.Unlock()

	ch.signalFlush()
	slog.Info("virtual connection opened", "conn_id", connID, "dst", dst, "channel", ch.channelID)
	return vc, nil
}

func (m *MuxClient) CloseConn(_ context.Context, vc *VirtualConn) {
	_ = vc.Close()
}

// CloseAll closes every conn and deletes the channels it owns.
func (m *MuxClient) CloseAll(ctx context.Context) {
	m.mu.Lock()
	conns := make([]*VirtualConn, 0, len(m.conns))
	for _, vc := range m.conns {
		conns = append(conns, vc)
	}
	m.conns = make(map[string]*VirtualConn)
	m.mu.Unlock()

	for _, vc := range conns {
		vc.closeOnce.Do(func() { close(vc.closed) })
	}

	// Best-effort delete of channels; ignore individual failures.
	for _, ch := range m.channels {
		if err := ch.transport.DeleteChannel(ctx, ch.channelID); err != nil {
			slog.Warn("cleanup channel failed", "channel_id", ch.channelID, "error", err)
		}
	}
}

// Snapshot returns a sorted (by start time) view of all active connections.
func (m *MuxClient) Snapshot() []ConnSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ConnSnapshot, 0, len(m.conns))
	for _, vc := range m.conns {
		ch := vc.channel
		transport := ""
		if ch != nil {
			transport = ch.transportKind
		}
		var tokenIdx int
		if ch != nil {
			tokenIdx = ch.tokenIdx
		}
		out = append(out, ConnSnapshot{
			ConnID:    vc.connID,
			Dst:       vc.dst,
			Transport: transport,
			TokenIdx:  tokenIdx,
			BytesUp:   vc.bytesUp.Load(),
			BytesDown: vc.bytesDown.Load(),
			StartTime: vc.startTime,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].StartTime.Before(out[j].StartTime)
	})
	return out
}

// batchWriteLoop drives one upstream channel's writer. Single-shot timer
// reset to batchInterval after each flush; flushSig may shorten it down
// to fastFlushGap (= batchInterval/4) for interactive responsiveness.
func (m *MuxClient) batchWriteLoop(ctx context.Context, ch *upstreamChannel) {
	batchInterval := ch.batchInterval
	if batchInterval <= 0 {
		batchInterval = m.cfg.GitHub.BatchInterval
	}
	if batchInterval <= 0 {
		batchInterval = 100 * time.Millisecond
	}

	fastFlushGap := batchInterval / 4
	if fastFlushGap < 20*time.Millisecond {
		fastFlushGap = 20 * time.Millisecond
	}
	if fastFlushGap > batchInterval {
		fastFlushGap = batchInterval
	}

	timer := time.NewTimer(batchInterval)
	defer timer.Stop()
	nextDeadline := time.Now().Add(batchInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.flushOutbound(ctx, ch)
			nextDeadline = time.Now().Add(batchInterval)
			timer.Reset(batchInterval)
		case <-ch.flushSig:
			target := time.Now().Add(fastFlushGap)
			if target.Before(nextDeadline) {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				d := time.Until(target)
				if d < 0 {
					d = 0
				}
				timer.Reset(d)
				nextDeadline = target
			}
		}
	}
}

func (m *MuxClient) flushOutbound(ctx context.Context, ch *upstreamChannel) {
	m.mu.RLock()
	snapshot := make([]*VirtualConn, 0, len(m.conns))
	for _, vc := range m.conns {
		if vc.channel == ch {
			snapshot = append(snapshot, vc)
		}
	}
	m.mu.RUnlock()

	frames := make([]shared.Frame, 0, len(snapshot))
	toDelete := make([]string, 0)

	for _, vc := range snapshot {
		vc.mu.Lock()
		queue := vc.writeQueue
		vc.writeQueue = nil
		vc.mu.Unlock()

		isClosed := false
		select {
		case <-vc.closed:
			isClosed = true
		default:
		}

		needsOpen := !vc.openSent.Load()

		// closed-before-open: server never knew about it, drop silently.
		if needsOpen && len(queue) == 0 && isClosed {
			toDelete = append(toDelete, vc.connID)
			continue
		}
		// close already flushed: final GC pass.
		if isClosed && vc.closeSent.Load() {
			toDelete = append(toDelete, vc.connID)
			continue
		}
		// nothing to do.
		if !needsOpen && len(queue) == 0 && !isClosed {
			continue
		}

		// bare OPEN (server-speaks-first protocols).
		if needsOpen && len(queue) == 0 {
			seq := vc.seq.Add(1)
			status := shared.FrameActive
			if isClosed {
				status = shared.FrameClosing
				vc.closeSent.Store(true)
				toDelete = append(toDelete, vc.connID)
			}
			frames = append(frames, shared.Frame{
				ConnID: vc.connID,
				Seq:    seq,
				Dst:    vc.dst,
				Status: status,
			})
			vc.openSent.Store(true)
			continue
		}

		// Coalesce all chunks into one frame (highest seq, concatenated data).
		var merged []byte
		var maxSeq int64
		for _, chunk := range queue {
			if len(chunk.data) > 0 {
				merged = append(merged, chunk.data...)
			}
			if chunk.seq > maxSeq {
				maxSeq = chunk.seq
			}
		}

		encoded := ""
		if len(merged) > 0 {
			enc, err := ch.encryptor.Encrypt(merged, vc.connID, maxSeq)
			if err != nil {
				slog.Warn("encrypt failed", "conn_id", vc.connID, "error", err)
				if isClosed {
					frames = append(frames, shared.Frame{
						ConnID: vc.connID,
						Seq:    vc.seq.Add(1),
						Status: shared.FrameClosing,
					})
					vc.closeSent.Store(true)
					toDelete = append(toDelete, vc.connID)
				}
				continue
			}
			encoded = enc
		}

		status := shared.FrameActive
		if isClosed {
			status = shared.FrameClosing
		}
		dst := ""
		if needsOpen {
			dst = vc.dst
		}
		frames = append(frames, shared.Frame{
			ConnID: vc.connID,
			Seq:    maxSeq,
			Dst:    dst,
			Data:   encoded,
			Status: status,
		})
		if needsOpen {
			vc.openSent.Store(true)
		}
		if isClosed {
			vc.closeSent.Store(true)
			toDelete = append(toDelete, vc.connID)
		}
	}

	if len(frames) == 0 {
		m.gcConns(toDelete)
		return
	}

	if err := m.acquireForToken(ctx, ch.tokenIdx); err != nil {
		slog.Debug("write skipped due to rate limiter", "token_idx", ch.tokenIdx, "error", err)
		return
	}

	seq := ch.batchSeq.Add(1)
	batch := &shared.Batch{
		Epoch:  ch.epoch,
		Seq:    seq,
		Ts:     time.Now().Unix(),
		Frames: frames,
	}
	if err := ch.transport.Write(ctx, ch.channelID, shared.ClientBatchFile, batch); err != nil {
		slog.Warn("batch write failed", "channel", ch.channelID, "error", err)
		return
	}

	m.afterTransportCall(ch.tokenIdx, ch.transport)

	// Write quotas only apply to gist; git has no equivalent cap.
	if ch.transportKind == "gist" {
		if wait := m.rateLimiter.RecordWrite(ch.tokenIdx); wait > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
		}
	}

	m.gcConns(toDelete)
}

func (m *MuxClient) gcConns(ids []string) {
	if len(ids) == 0 {
		return
	}
	m.mu.Lock()
	for _, id := range ids {
		delete(m.conns, id)
	}
	m.mu.Unlock()
}

// batchReadLoop polls server.json for new batches; backs off when idle.
func (m *MuxClient) batchReadLoop(ctx context.Context, ch *upstreamChannel) {
	fetchInterval := ch.fetchInterval
	if fetchInterval <= 0 {
		fetchInterval = m.cfg.GitHub.FetchInterval
	}
	if fetchInterval <= 0 {
		fetchInterval = 200 * time.Millisecond
	}

	timer := time.NewTimer(fetchInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			m.doBatchRead(ctx, ch)

			m.mu.RLock()
			active := 0
			for _, vc := range m.conns {
				if vc.channel == ch {
					active++
					if active > 0 {
						break
					}
				}
			}
			m.mu.RUnlock()

			interval := fetchInterval
			if active == 0 {
				interval *= 10
				if interval > 5*time.Second {
					interval = 5 * time.Second
				}
			}
			timer.Reset(interval)
		}
	}
}

func (m *MuxClient) doBatchRead(ctx context.Context, ch *upstreamChannel) {
	if err := m.acquireForToken(ctx, ch.tokenIdx); err != nil {
		slog.Debug("read skipped due to rate limiter", "token_idx", ch.tokenIdx, "error", err)
		return
	}
	batch, err := ch.transport.Read(ctx, ch.channelID, shared.ServerBatchFile)
	m.afterTransportCall(ch.tokenIdx, ch.transport)
	if err != nil {
		slog.Debug("batch read failed", "channel", ch.channelID, "error", err)
		return
	}
	if batch == nil {
		return
	}

	lastEpoch := ch.lastReadEpoch.Load()
	lastSeq := ch.lastReadSeq.Load()
	if batch.Epoch != lastEpoch {
		ch.lastReadEpoch.Store(batch.Epoch)
		ch.lastReadSeq.Store(batch.Seq)
		slog.Info("server batch epoch changed; resetting dedup",
			"channel", ch.channelID, "old_epoch", lastEpoch, "new_epoch", batch.Epoch)
	} else if batch.Seq <= lastSeq {
		return
	} else {
		ch.lastReadSeq.Store(batch.Seq)
	}

	m.dispatchFrames(ch, batch.Frames)
}

// dispatchFrames hands each server frame to its VirtualConn. The conn-map
// RLock is dropped before delivery so a slow consumer can't freeze Connect
// or CloseAll. We block on vc.recvBuf rather than spawning per-frame
// goroutines so per-conn frame ordering is preserved.
func (m *MuxClient) dispatchFrames(ch *upstreamChannel, frames []shared.Frame) {
	for _, f := range frames {
		m.mu.RLock()
		vc, ok := m.conns[f.ConnID]
		m.mu.RUnlock()
		if !ok {
			continue
		}

		switch f.Status {
		case shared.FrameClosed, shared.FrameError:
			if f.Status == shared.FrameError && f.Error != "" {
				slog.Info("server reported error for conn", "conn_id", f.ConnID, "error", f.Error)
			}
			vc.closeOnce.Do(func() { close(vc.closed) })
			// server already tore down — don't echo a Closing.
			vc.closeSent.Store(true)
			continue
		}

		if f.Data == "" {
			continue
		}
		plaintext, err := ch.encryptor.Decrypt(f.Data, f.ConnID, f.Seq)
		if err != nil {
			slog.Warn("decrypt failed", "conn_id", f.ConnID, "seq", f.Seq, "error", err)
			continue
		}
		if len(plaintext) == 0 {
			continue
		}

		select {
		case vc.recvBuf <- plaintext:
		case <-vc.closed:
		}
	}
}

func (m *MuxClient) initTokenChannels(ctx context.Context, tokenIdx int, transport shared.Transport, count int) ([]*upstreamChannel, error) {
	token := m.rateLimiter.GetToken(tokenIdx)
	encryptor := shared.NewEncryptor(shared.EncryptionAlgorithm(m.cfg.Encryption.Algorithm), token)

	var tc TokenConfig
	if tokenIdx < len(m.cfg.GitHub.Tokens) {
		tc = m.cfg.GitHub.Tokens[tokenIdx]
	}
	transportKind := tc.EffectiveTransport()
	batchInterval := tc.EffectiveBatchInterval(m.cfg.GitHub.BatchInterval)
	fetchInterval := tc.EffectiveFetchInterval(m.cfg.GitHub.FetchInterval)
	epoch := randomEpoch()

	channels := make([]*upstreamChannel, 0, count)
	for len(channels) < count {
		if err := m.acquireForToken(ctx, tokenIdx); err != nil {
			return channels, err
		}
		chID, err := transport.EnsureChannel(ctx, "")
		m.afterTransportCall(tokenIdx, transport)
		if err != nil {
			return channels, fmt.Errorf("token %d channel %d: %w", tokenIdx, len(channels), err)
		}
		channels = append(channels, &upstreamChannel{
			channelID:     chID,
			tokenIdx:      tokenIdx,
			transport:     transport,
			encryptor:     encryptor,
			transportKind: transportKind,
			batchInterval: batchInterval,
			fetchInterval: fetchInterval,
			epoch:         epoch,
			flushSig:      make(chan struct{}, 1),
		})
		slog.Info("upstream channel ready",
			"channel_id", chID,
			"token_idx", tokenIdx,
			"transport", transportKind,
			"batch_interval", batchInterval.String(),
			"fetch_interval", fetchInterval.String(),
		)
	}

	return channels, nil
}

func (m *MuxClient) acquireForToken(ctx context.Context, tokenIdx int) error {
	if tokenIdx >= len(m.cfg.GitHub.Tokens) {
		return nil
	}
	if m.cfg.GitHub.Tokens[tokenIdx].EffectiveTransport() != "gist" {
		return nil
	}
	return m.rateLimiter.Acquire(ctx, tokenIdx)
}

func (m *MuxClient) afterTransportCall(tokenIdx int, transport shared.Transport) {
	m.rateLimiter.RecordTransportCall(tokenIdx)

	if tokenIdx >= len(m.cfg.GitHub.Tokens) {
		return
	}
	if m.cfg.GitHub.Tokens[tokenIdx].EffectiveTransport() != "gist" {
		return
	}
	m.rateLimiter.UpdateFromHeaders(tokenIdx, transport.GetRateLimitInfo())
}

// randomEpoch returns a non-zero random int64 for Batch.Epoch.
func randomEpoch() int64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	v := int64(binary.BigEndian.Uint64(b[:]))
	if v == 0 {
		return 1
	}
	return v
}

var _ io.ReadWriteCloser = (*VirtualConn)(nil)
