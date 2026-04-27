package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sartoopjj/vpn-over-github/shared"
)

// destConn tracks one upstream destination tunnelled through this channel.
//
// Created on the first OPEN frame for that connID. The dial happens in a
// goroutine — concurrent client frames for the same conn are buffered in
// earlyData and replayed once dial completes. writeCh feeds a per-conn
// writer goroutine so a slow upstream Write only blocks its own conn.
type destConn struct {
	connID    string
	dst       string
	conn      net.Conn
	seq       atomic.Int64
	closed    chan struct{}
	closeOnce sync.Once

	mu        sync.Mutex
	pending   []shared.Frame
	earlyData []shared.Frame
	dialed    bool
	dialErr   error

	writeCh chan []byte
}

func (d *destConn) enqueue(f shared.Frame) {
	d.mu.Lock()
	d.pending = append(d.pending, f)
	d.mu.Unlock()
}

func (d *destConn) drain() []shared.Frame {
	d.mu.Lock()
	out := d.pending
	d.pending = nil
	d.mu.Unlock()
	return out
}

func (d *destConn) close() {
	d.closeOnce.Do(func() {
		close(d.closed)
		// dc.conn is written by dialAsync under d.mu; read under it too.
		d.mu.Lock()
		c := d.conn
		d.mu.Unlock()
		if c != nil {
			_ = c.Close()
		}
	})
}

// ChannelHandler owns one upstream channel: reads client.json, dispatches
// to per-conn destinations, and writes server.json.
type ChannelHandler struct {
	cfg       *ServerConfig
	channelID string
	tokenIdx  int
	transport shared.Transport
	encryptor *shared.Encryptor

	transportKind string
	batchInterval time.Duration
	fetchInterval time.Duration

	epoch          int64
	serverBatchSeq atomic.Int64

	lastClientEpoch atomic.Int64
	lastClientSeq   atomic.Int64
	hadFirstRead    atomic.Bool

	mu    sync.RWMutex
	conns map[string]*destConn

	flushSig chan struct{}
}

func NewChannelHandler(cfg *ServerConfig, channelID string, tokenIdx int, transport shared.Transport, token string) *ChannelHandler {
	transportKind := "git"
	batchInterval := cfg.GitHub.BatchInterval
	fetchInterval := cfg.GitHub.FetchInterval
	if tokenIdx < len(cfg.GitHub.Tokens) {
		tc := cfg.GitHub.Tokens[tokenIdx]
		transportKind = tc.EffectiveTransport()
		batchInterval = tc.EffectiveBatchInterval(batchInterval)
		fetchInterval = tc.EffectiveFetchInterval(fetchInterval)
	}
	return &ChannelHandler{
		cfg:           cfg,
		channelID:     channelID,
		tokenIdx:      tokenIdx,
		transport:     transport,
		encryptor:     shared.NewEncryptor(shared.EncryptionAlgorithm(cfg.Encryption.Algorithm), token),
		transportKind: transportKind,
		batchInterval: batchInterval,
		fetchInterval: fetchInterval,
		epoch:         randomEpoch(),
		conns:         make(map[string]*destConn),
		flushSig:      make(chan struct{}, 1),
	}
}

func (h *ChannelHandler) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		h.readClientLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		h.writeServerLoop(ctx)
	}()

	wg.Wait()
	h.closeAll()
}

func (h *ChannelHandler) signalFlush() {
	select {
	case h.flushSig <- struct{}{}:
	default:
	}
}

func (h *ChannelHandler) readClientLoop(ctx context.Context) {
	timer := time.NewTimer(h.fetchInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if !h.doReadClient(ctx) {
				return
			}

			h.mu.RLock()
			active := len(h.conns)
			h.mu.RUnlock()

			interval := h.fetchInterval
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

func (h *ChannelHandler) doReadClient(ctx context.Context) bool {
	batch, err := h.transport.Read(ctx, h.channelID, shared.ClientBatchFile)
	if err != nil {
		if errors.Is(err, shared.ErrNotFound) {
			slog.Debug("client batch not found yet", "channel_id", h.channelID)
			return true
		}
		slog.Debug("read client batch failed", "channel_id", h.channelID, "error", err)
		return true
	}
	if batch == nil {
		return true
	}

	lastEpoch := h.lastClientEpoch.Load()
	lastSeq := h.lastClientSeq.Load()
	if batch.Epoch != lastEpoch {
		h.lastClientEpoch.Store(batch.Epoch)
		h.lastClientSeq.Store(batch.Seq)
		if h.hadFirstRead.Load() {
			slog.Info("client batch epoch changed; resetting dedup",
				"channel_id", h.channelID, "old_epoch", lastEpoch, "new_epoch", batch.Epoch)
		}
	} else if batch.Seq <= lastSeq {
		return true
	} else {
		h.lastClientSeq.Store(batch.Seq)
	}

	if !h.hadFirstRead.Load() {
		h.hadFirstRead.Store(true)
		if batch.Age() > 60*time.Second {
			slog.Debug("skipping stale client batch on startup",
				"channel_id", h.channelID, "age", batch.Age().Round(time.Second))
			return true
		}
	}

	for _, f := range batch.Frames {
		h.handleClientFrame(ctx, f)
	}
	return true
}

// handleClientFrame routes one client frame. New conns spawn an async dial
// and buffer subsequent frames until the dial completes.
func (h *ChannelHandler) handleClientFrame(ctx context.Context, f shared.Frame) {
	h.mu.RLock()
	dc, ok := h.conns[f.ConnID]
	h.mu.RUnlock()

	if !ok {
		if f.Dst == "" {
			slog.Debug("data frame for unknown conn", "channel_id", h.channelID, "conn_id", f.ConnID)
			return
		}
		newDC := &destConn{
			connID:  f.ConnID,
			dst:     f.Dst,
			closed:  make(chan struct{}),
			writeCh: make(chan []byte, 64),
		}
		h.mu.Lock()
		if existing, exists := h.conns[f.ConnID]; exists {
			dc = existing
			h.mu.Unlock()
		} else {
			h.conns[f.ConnID] = newDC
			dc = newDC
			h.mu.Unlock()
			go h.dialAsync(ctx, newDC)
		}
	}

	// Capture dialed/dialErr under the lock — they're set by dialAsync.
	dc.mu.Lock()
	dialed := dc.dialed
	dialErr := dc.dialErr
	if !dialed {
		dc.earlyData = append(dc.earlyData, f)
		dc.mu.Unlock()
		return
	}
	dc.mu.Unlock()

	if dialErr != nil {
		return
	}
	h.processClientFrame(dc, f)
}

// dialAsync dials dc.dst then replays buffered frames. We DO NOT set
// dialed=true until the earlyData drain finishes — otherwise a new frame
// hitting the fast path during replay would race the replay's writes to
// dc.writeCh and break ordering.
func (h *ChannelHandler) dialAsync(ctx context.Context, dc *destConn) {
	conn, err := h.dialDestination(ctx, dc.dst)

	if err != nil {
		dc.mu.Lock()
		dc.dialed = true
		dc.dialErr = err
		dc.earlyData = nil
		dc.mu.Unlock()

		slog.Warn("dial failed",
			"channel_id", h.channelID,
			"conn_id", dc.connID,
			"dst", dc.dst,
			"error", err,
		)
		dc.enqueue(shared.Frame{
			ConnID: dc.connID,
			Seq:    dc.seq.Add(1),
			Status: shared.FrameError,
			Error:  err.Error(),
		})
		dc.close()
		h.signalFlush()
		return
	}

	dc.mu.Lock()
	dc.conn = conn
	dc.mu.Unlock()

	slog.Info("server opened destination",
		"channel_id", h.channelID,
		"conn_id", dc.connID,
		"dst", dc.dst,
	)
	go h.readDestLoop(ctx, dc)
	go h.writeDestLoop(ctx, dc)

	dc.mu.Lock()
	for {
		early := dc.earlyData
		dc.earlyData = nil
		if len(early) == 0 {
			dc.dialed = true
			dc.mu.Unlock()
			return
		}
		dc.mu.Unlock()
		for _, f := range early {
			h.processClientFrame(dc, f)
		}
		dc.mu.Lock()
	}
}

// processClientFrame applies one frame to a dialed destConn. The TCP write
// is handed to writeDestLoop so a slow target only stalls its own conn.
func (h *ChannelHandler) processClientFrame(dc *destConn, f shared.Frame) {
	switch f.Status {
	case shared.FrameClosing, shared.FrameClosed, shared.FrameError:
		dc.close()
		dc.enqueue(shared.Frame{
			ConnID: dc.connID,
			Seq:    dc.seq.Add(1),
			Status: shared.FrameClosed,
		})
		h.signalFlush()
		return
	}

	if f.Data == "" {
		return
	}

	plaintext, err := h.encryptor.Decrypt(f.Data, f.ConnID, f.Seq)
	if err != nil {
		dc.enqueue(shared.Frame{
			ConnID: dc.connID,
			Seq:    dc.seq.Add(1),
			Status: shared.FrameError,
			Error:  "decrypt failed",
		})
		dc.close()
		h.signalFlush()
		return
	}
	if len(plaintext) == 0 {
		return
	}

	select {
	case dc.writeCh <- plaintext:
	case <-dc.closed:
	}
}

// writeDestLoop drains dc.writeCh and serially writes to dc.conn. Started
// once per conn after dialAsync succeeds.
func (h *ChannelHandler) writeDestLoop(ctx context.Context, dc *destConn) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-dc.closed:
			return
		case data, ok := <-dc.writeCh:
			if !ok {
				return
			}
			_ = dc.conn.SetWriteDeadline(time.Now().Add(h.cfg.Proxy.TargetTimeout))
			if _, err := dc.conn.Write(data); err != nil {
				dc.enqueue(shared.Frame{
					ConnID: dc.connID,
					Seq:    dc.seq.Add(1),
					Status: shared.FrameError,
					Error:  "destination write failed",
				})
				dc.close()
				h.signalFlush()
				return
			}
		}
	}
}

func (h *ChannelHandler) readDestLoop(ctx context.Context, dc *destConn) {
	buf := make([]byte, h.cfg.Proxy.BufferSize)
	for {
		select {
		case <-ctx.Done():
			return
		case <-dc.closed:
			return
		default:
		}
		_ = dc.conn.SetReadDeadline(time.Now().Add(h.cfg.Proxy.TargetTimeout))
		n, err := dc.conn.Read(buf)
		if n > 0 {
			payload := make([]byte, n)
			copy(payload, buf[:n])
			seq := dc.seq.Add(1)
			enc, encErr := h.encryptor.Encrypt(payload, dc.connID, seq)
			if encErr != nil {
				dc.enqueue(shared.Frame{
					ConnID: dc.connID,
					Seq:    dc.seq.Add(1),
					Status: shared.FrameError,
					Error:  "encrypt failed",
				})
			} else {
				dc.enqueue(shared.Frame{
					ConnID: dc.connID,
					Seq:    seq,
					Data:   enc,
					Status: shared.FrameActive,
				})
			}
			h.signalFlush()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				dc.enqueue(shared.Frame{
					ConnID: dc.connID,
					Seq:    dc.seq.Add(1),
					Status: shared.FrameClosed,
				})
				dc.close()
				h.signalFlush()
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			dc.enqueue(shared.Frame{
				ConnID: dc.connID,
				Seq:    dc.seq.Add(1),
				Status: shared.FrameError,
				Error:  "destination read failed",
			})
			dc.close()
			h.signalFlush()
			return
		}
	}
}

func (h *ChannelHandler) writeServerLoop(ctx context.Context) {
	batchInterval := h.batchInterval
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
			h.flushServerFrames(ctx)
			nextDeadline = time.Now().Add(batchInterval)
			timer.Reset(batchInterval)
		case <-h.flushSig:
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

func (h *ChannelHandler) flushServerFrames(ctx context.Context) {
	h.mu.RLock()
	conns := make([]*destConn, 0, len(h.conns))
	for _, dc := range h.conns {
		conns = append(conns, dc)
	}
	h.mu.RUnlock()

	frames := make([]shared.Frame, 0)
	var toGC []string

	for _, dc := range conns {
		drained := dc.drain()
		frames = append(frames, drained...)

		select {
		case <-dc.closed:
			dc.mu.Lock()
			pendingEmpty := len(dc.pending) == 0
			dc.mu.Unlock()
			if pendingEmpty {
				toGC = append(toGC, dc.connID)
			}
		default:
		}
	}

	if len(frames) == 0 {
		h.gcConns(toGC)
		return
	}

	seq := h.serverBatchSeq.Add(1)
	batch := &shared.Batch{
		Epoch:  h.epoch,
		Seq:    seq,
		Ts:     time.Now().Unix(),
		Frames: frames,
	}
	if err := h.transport.Write(ctx, h.channelID, shared.ServerBatchFile, batch); err != nil {
		slog.Debug("write server batch failed", "channel_id", h.channelID, "error", err)
		return
	}

	h.gcConns(toGC)
}

func (h *ChannelHandler) gcConns(ids []string) {
	if len(ids) == 0 {
		return
	}
	h.mu.Lock()
	for _, id := range ids {
		// Re-check pending: readDestLoop may have appended after we drained.
		if dc, ok := h.conns[id]; ok {
			dc.mu.Lock()
			pendingEmpty := len(dc.pending) == 0
			dc.mu.Unlock()
			if pendingEmpty {
				delete(h.conns, id)
			}
		}
	}
	h.mu.Unlock()
}

func (h *ChannelHandler) dialDestination(ctx context.Context, dst string) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, h.cfg.Proxy.TargetTimeout)
	defer cancel()
	return (&net.Dialer{}).DialContext(dialCtx, "tcp", dst)
}

func (h *ChannelHandler) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for connID, dc := range h.conns {
		dc.close()
		delete(h.conns, connID)
	}
}

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
