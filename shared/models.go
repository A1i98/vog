package shared

import "time"

const (
	ChannelDescPrefix = "gist-tunnel-ch"
	ClientBatchFile   = "client.json"
	ServerBatchFile   = "server.json"
)

// FrameStatus is the state of a virtual connection within the mux.
type FrameStatus string

const (
	FrameActive  FrameStatus = "active"
	FrameClosing FrameStatus = "closing"
	FrameClosed  FrameStatus = "closed"
	FrameError   FrameStatus = "error"
)

// Frame carries data for one virtual connection within a multiplexed Batch.
type Frame struct {
	ConnID string      `json:"id"`
	Seq    int64       `json:"seq"`
	Dst    string      `json:"dst,omitempty"`
	Data   string      `json:"data,omitempty"` // base64-encoded encrypted payload
	Status FrameStatus `json:"status"`
	Error  string      `json:"err,omitempty"`
}

// Batch is what each side writes to its channel file. The writer picks a
// random Epoch at startup and increments Seq monotonically within that
// epoch; readers accept a batch when (epoch != last) || (seq > last).
type Batch struct {
	Epoch  int64   `json:"epoch,omitempty"`
	Seq    int64   `json:"seq"`
	Ts     int64   `json:"ts"`
	Frames []Frame `json:"frames"`
}

// ChannelInfo describes one transport channel (gist or git directory).
type ChannelInfo struct {
	ID          string
	Description string
	UpdatedAt   time.Time
}

// TokenState holds per-token rate-limit and write-counter state.
type TokenState struct {
	Token              string
	RateLimitRemaining int
	RateLimitTotal     int
	RateLimitReset     time.Time
	BackoffUntil       time.Time
	BackoffLevel       int
	LastSecondStart    time.Time
	RequestsThisSecond int
	WriteMinuteStart   time.Time
	WritesThisMinute   int
	WriteHourStart     time.Time
	WritesThisHour     int
	TotalAPICalls      int64
	Priority           int
}

func MaskToken(token string) string {
	if len(token) <= 8 {
		return "****"
	}
	return token[:4] + "****" + token[len(token)-4:]
}

// Age returns time.Since(batch.Ts).
func (b *Batch) Age() time.Duration {
	if b == nil || b.Ts == 0 {
		return 0
	}
	return time.Since(time.Unix(b.Ts, 0))
}
