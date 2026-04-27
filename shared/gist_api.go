package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const githubAPIBase = "https://api.github.com"

// Transport is the abstraction over all tunnel data storage backends.
type Transport interface {
	EnsureChannel(ctx context.Context, existingID string) (string, error)
	DeleteChannel(ctx context.Context, channelID string) error
	ListChannels(ctx context.Context) ([]*ChannelInfo, error)
	Write(ctx context.Context, channelID, filename string, batch *Batch) error
	Read(ctx context.Context, channelID, filename string) (*Batch, error)
	GetRateLimitInfo() RateLimitInfo
}

// RateLimitInfo holds the rate limit state from GitHub response headers.
type RateLimitInfo struct {
	Remaining int
	Limit     int
	// Resource is the X-RateLimit-Resource bucket (e.g. "core", "search").
	// Only "core" (or empty) applies to gist/repo REST calls.
	Resource    string
	ResetAt     time.Time
	RetryAfter  time.Time
	LastUpdated time.Time
}

// HTTPClient abstracts net/http.Client to allow test injection.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// readCacheEntry caches a parsed Batch and its ETag for conditional GETs.
type readCacheEntry struct {
	etag  string
	batch *Batch
}

// GitHubGistClient implements Transport using the GitHub Gist REST API.
type GitHubGistClient struct {
	token     string
	client    HTTPClient
	rateLimit RateLimitInfo

	mu        sync.Mutex
	readCache map[string]*readCacheEntry // key: channelID+"/"+filename
	listETag  string
	listCache []*ChannelInfo
}

// NewGitHubGistClient creates a GitHubGistClient.
func NewGitHubGistClient(token string, httpClient HTTPClient) *GitHubGistClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &GitHubGistClient{
		token:     token,
		client:    httpClient,
		readCache: make(map[string]*readCacheEntry),
		rateLimit: RateLimitInfo{Remaining: 5000},
	}
}

var (
	ErrNotFound           = fmt.Errorf("channel not found")
	ErrRateLimit          = fmt.Errorf("github API rate limit exceeded")
	ErrSecondaryRateLimit = fmt.Errorf("github API secondary rate limit exceeded")
	ErrForbidden          = fmt.Errorf("access forbidden (verify token has 'gist' scope)")
)

type githubGistResponse struct {
	ID          string                        `json:"id"`
	Description string                        `json:"description"`
	Files       map[string]githubGistFileResp `json:"files"`
	UpdatedAt   time.Time                     `json:"updated_at"`
}

type githubGistFileResp struct {
	Filename string `json:"filename"`
	Content  string `json:"content"`
}

type githubErrorResp struct {
	Message string `json:"message"`
}

// EnsureChannel creates a channel gist if it doesn't exist, or verifies an existing one.
func (c *GitHubGistClient) EnsureChannel(ctx context.Context, existingID string) (string, error) {
	if existingID != "" {
		resp, err := c.doRequest(ctx, http.MethodGet, "/gists/"+existingID, nil, "")
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return existingID, nil
		}
	}

	body := map[string]interface{}{
		"description": ChannelDescPrefix,
		"public":      false,
		"files": map[string]map[string]string{
			ClientBatchFile: {"content": `{"seq":0,"ts":0,"frames":[]}`},
			ServerBatchFile: {"content": `{"seq":0,"ts":0,"frames":[]}`},
		},
	}
	resp, err := c.doRequest(ctx, http.MethodPost, "/gists", body, "")
	if err != nil {
		return "", fmt.Errorf("EnsureChannel create: %w", err)
	}
	defer resp.Body.Close()
	if err := c.checkStatus(resp, http.StatusCreated); err != nil {
		return "", fmt.Errorf("EnsureChannel: %w", err)
	}
	var gr githubGistResponse
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		return "", fmt.Errorf("EnsureChannel decode: %w", err)
	}
	return gr.ID, nil
}

// DeleteChannel permanently removes a channel gist. Treats 404 as success.
func (c *GitHubGistClient) DeleteChannel(ctx context.Context, channelID string) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, "/gists/"+channelID, nil, "")
	if err != nil {
		return fmt.Errorf("DeleteChannel %s: %w", channelID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if err := c.checkStatus(resp, http.StatusNoContent); err != nil {
		return fmt.Errorf("DeleteChannel %s: %w", channelID, err)
	}
	return nil
}

// ListChannels returns all gists with description matching ChannelDescPrefix.
// Uses ETag caching to avoid counting unchanged lists against rate limit.
func (c *GitHubGistClient) ListChannels(ctx context.Context) ([]*ChannelInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubAPIBase+"/gists?per_page=100", nil)
	if err != nil {
		return nil, fmt.Errorf("ListChannels build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	c.mu.Lock()
	if c.listETag != "" {
		req.Header.Set("If-None-Match", c.listETag)
	}
	c.mu.Unlock()

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ListChannels request: %w", err)
	}
	defer resp.Body.Close()
	c.updateRateLimitFromHeaders(resp)

	if resp.StatusCode == http.StatusNotModified {
		c.mu.Lock()
		cached := c.listCache
		c.mu.Unlock()
		return cached, nil
	}
	if err := c.checkStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("ListChannels: %w", err)
	}

	var gists []githubGistResponse
	if err := json.NewDecoder(resp.Body).Decode(&gists); err != nil {
		return nil, fmt.Errorf("ListChannels decode: %w", err)
	}

	var channels []*ChannelInfo
	for _, g := range gists {
		if strings.HasPrefix(g.Description, ChannelDescPrefix) {
			channels = append(channels, &ChannelInfo{
				ID:          g.ID,
				Description: g.Description,
				UpdatedAt:   g.UpdatedAt,
			})
		}
	}

	c.mu.Lock()
	if etag := resp.Header.Get("ETag"); etag != "" {
		c.listETag = etag
	}
	c.listCache = channels
	c.mu.Unlock()
	return channels, nil
}

// Write serialises batch (gzip when worthwhile) and patches channelID/filename
// in the gist.
func (c *GitHubGistClient) Write(ctx context.Context, channelID, filename string, batch *Batch) error {
	content, err := EncodeBatchString(batch)
	if err != nil {
		return fmt.Errorf("Write encode: %w", err)
	}
	body := map[string]interface{}{
		"files": map[string]map[string]string{
			filename: {"content": content},
		},
	}
	resp, err := c.doRequest(ctx, http.MethodPatch, "/gists/"+channelID, body, "")
	if err != nil {
		return fmt.Errorf("Write %s/%s request: %w", channelID, filename, err)
	}
	defer resp.Body.Close()
	if err := c.checkStatus(resp, http.StatusOK); err != nil {
		return fmt.Errorf("Write %s/%s: %w", channelID, filename, err)
	}
	c.mu.Lock()
	delete(c.readCache, channelID+"/"+filename)
	c.mu.Unlock()
	return nil
}

// Read fetches and parses channelID/filename. Returns (nil, nil) on 304 or empty content.
func (c *GitHubGistClient) Read(ctx context.Context, channelID, filename string) (*Batch, error) {
	cacheKey := channelID + "/" + filename

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubAPIBase+"/gists/"+channelID, nil)
	if err != nil {
		return nil, fmt.Errorf("Read build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	c.mu.Lock()
	entry := c.readCache[cacheKey]
	if entry != nil && entry.etag != "" {
		req.Header.Set("If-None-Match", entry.etag)
	}
	c.mu.Unlock()

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Read %s/%s request: %w", channelID, filename, err)
	}
	defer resp.Body.Close()
	c.updateRateLimitFromHeaders(resp)

	if resp.StatusCode == http.StatusNotModified {
		c.mu.Lock()
		var batch *Batch
		if entry != nil {
			batch = entry.batch
		}
		c.mu.Unlock()
		return batch, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if err := c.checkStatus(resp, http.StatusOK); err != nil {
		return nil, fmt.Errorf("Read %s/%s: %w", channelID, filename, err)
	}

	var gr githubGistResponse
	if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
		return nil, fmt.Errorf("Read %s/%s decode: %w", channelID, filename, err)
	}

	f, ok := gr.Files[filename]
	if !ok || f.Content == "" {
		return nil, nil
	}

	batch, err := DecodeBatchString(f.Content)
	if err != nil {
		return nil, fmt.Errorf("Read %s/%s parse batch: %w", channelID, filename, err)
	}

	etag := resp.Header.Get("ETag")
	c.mu.Lock()
	c.readCache[cacheKey] = &readCacheEntry{etag: etag, batch: batch}
	c.mu.Unlock()
	return batch, nil
}

func (c *GitHubGistClient) GetRateLimitInfo() RateLimitInfo {
	return c.rateLimit
}

// ── internal helpers ─────────────────────────────────────────────────────────

func (c *GitHubGistClient) doRequest(ctx context.Context, method, path string, body interface{}, etag string) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, githubAPIBase+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP %s %s: %w", method, path, err)
	}
	c.updateRateLimitFromHeaders(resp)
	return resp, nil
}

func parseRateLimitHeaders(resp *http.Response) RateLimitInfo {
	var info RateLimitInfo
	if v, err := strconv.Atoi(resp.Header.Get("X-RateLimit-Remaining")); err == nil {
		info.Remaining = v
	}
	if v, err := strconv.Atoi(resp.Header.Get("X-RateLimit-Limit")); err == nil {
		info.Limit = v
	}
	if v, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil {
		info.ResetAt = time.Unix(v, 0)
	}
	info.Resource = resp.Header.Get("X-RateLimit-Resource")
	if ra, err := strconv.ParseInt(resp.Header.Get("Retry-After"), 10, 64); err == nil {
		info.RetryAfter = time.Now().Add(time.Duration(ra) * time.Second)
	}
	info.LastUpdated = time.Now()
	return info
}

// updateRateLimitFromHeaders parses X-RateLimit-* and Retry-After headers.
func (c *GitHubGistClient) updateRateLimitFromHeaders(resp *http.Response) {
	info := parseRateLimitHeaders(resp)
	if info.Limit <= 0 {
		return
	}
	// Reject non-core buckets ("search", "graphql", etc.) — different quota counters.
	if info.Resource != "" && info.Resource != "core" {
		return
	}
	c.rateLimit.Limit = info.Limit
	c.rateLimit.Remaining = info.Remaining
	if !info.ResetAt.IsZero() {
		c.rateLimit.ResetAt = info.ResetAt
	}
	if !info.RetryAfter.IsZero() {
		c.rateLimit.RetryAfter = info.RetryAfter
	}
	c.rateLimit.LastUpdated = info.LastUpdated
}

func (c *GitHubGistClient) checkStatus(resp *http.Response, expected int) error {
	if resp.StatusCode == expected {
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var errResp githubErrorResp
	_ = json.Unmarshal(raw, &errResp)

	switch resp.StatusCode {
	case http.StatusForbidden:
		if resp.Header.Get("X-RateLimit-Remaining") == "0" {
			return ErrRateLimit
		}
		if isSecondaryRateLimitMsg(errResp.Message) {
			return ErrSecondaryRateLimit
		}
		return fmt.Errorf("%w: %s", ErrForbidden, errResp.Message)
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusUnprocessableEntity:
		return fmt.Errorf("github validation failed (422): %s", errResp.Message)
	case http.StatusTooManyRequests:
		if resp.Header.Get("Retry-After") != "" || isSecondaryRateLimitMsg(errResp.Message) {
			return ErrSecondaryRateLimit
		}
		return ErrRateLimit
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable:
		return fmt.Errorf("github server error (%d): %s", resp.StatusCode, errResp.Message)
	default:
		return fmt.Errorf("unexpected HTTP %d: %s", resp.StatusCode, errResp.Message)
	}
}

func isSecondaryRateLimitMsg(msg string) bool {
	msg = strings.ToLower(msg)
	return strings.Contains(msg, "secondary rate limit") ||
		strings.Contains(msg, "exceeded a secondary")
}
