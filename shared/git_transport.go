package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gogit "github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

const (
	channelsDir       = "channels"
	syncInterval      = 200 * time.Millisecond
	writeBatchWindow  = 30 * time.Millisecond
	maxPushRetries    = 4
	speculativePushOK = true
)

type writeRequest struct {
	writes []writeEntry
	done   chan error
}

type writeEntry struct {
	relPath string
	content []byte
}

// GitSmartHTTPClient implements Transport via git push/pull (Smart HTTP).
//
// Locking is load-bearing: go-git's *Repository is NOT safe for concurrent
// fetch/push/commit/reset (we hit a real "concurrent map writes" panic in
// idxfile.MemoryIndex.FindOffset otherwise).
//
//   - repoMu     — exclusive; held for any go-git call.
//   - worktreeMu — RWMutex; RLock for Read/ListChannels (pure os ops),
//                  Lock for the brief worktree mutations (Reset, file write).
//
// worktreeMu is released BEFORE the network push, so Reads run during push
// and only block briefly during the reset/stage step. Lock order is always
// repoMu → worktreeMu.
type GitSmartHTTPClient struct {
	auth       *githttp.BasicAuth
	repoURL    string
	mainBranch string

	repoMu     sync.Mutex
	worktreeMu sync.RWMutex

	repo    *gogit.Repository
	workDir string
	writeCh chan *writeRequest
	cancel  context.CancelFunc
}

func NewGitSmartHTTPClient(token, repo string) (*GitSmartHTTPClient, error) {
	parts := strings.SplitN(repo, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("git transport: repo must be 'owner/repo' format, got %q", repo)
	}

	repoURL := "https://github.com/" + repo + ".git"
	auth := &githttp.BasicAuth{
		Username: "x-token-auth",
		Password: token,
	}

	workDir, err := os.MkdirTemp("", "gh-tunnel-git-*")
	if err != nil {
		return nil, fmt.Errorf("git transport: create workdir: %w", err)
	}

	r, err := gogit.PlainClone(workDir, false, &gogit.CloneOptions{
		URL:  repoURL,
		Auth: auth,
	})
	if err != nil {
		if !isEmptyRepoError(err) {
			_ = os.RemoveAll(workDir)
			return nil, fmt.Errorf("git transport: clone %s: %w", repo, err)
		}
		r, err = seedEmptyRepo(workDir, repoURL, auth)
		if err != nil {
			_ = os.RemoveAll(workDir)
			return nil, fmt.Errorf("git transport: seed empty repo %s: %w", repo, err)
		}
	}

	mainBranch := "main"
	if head, herr := r.Head(); herr == nil {
		mainBranch = head.Name().Short()
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &GitSmartHTTPClient{
		auth:       auth,
		repoURL:    repoURL,
		mainBranch: mainBranch,
		repo:       r,
		workDir:    workDir,
		writeCh:    make(chan *writeRequest, 256),
		cancel:     cancel,
	}
	go c.runSyncer(ctx)
	go c.runBatchWriter(ctx)
	return c, nil
}

func (c *GitSmartHTTPClient) Close() error {
	c.cancel()
	return os.RemoveAll(c.workDir)
}

// ── Transport interface ───────────────────────────────────────────────────────

// EnsureChannel creates channels/{id}/client.json and channels/{id}/server.json
// with placeholder content and pushes. If existingID is non-empty and the
// directory already exists locally, it is returned as-is.
func (c *GitSmartHTTPClient) EnsureChannel(ctx context.Context, existingID string) (string, error) {
	if existingID != "" {
		dir := filepath.Join(c.workDir, channelsDir, existingID)
		if _, err := os.Stat(dir); err == nil {
			return existingID, nil
		}
	}

	id, err := GenerateID()
	if err != nil {
		return "", fmt.Errorf("EnsureChannel generate ID: %w", err)
	}
	if existingID != "" {
		id = existingID
	}

	placeholder, err := EncodeBatchBytes(&Batch{Seq: 0, Ts: 0, Frames: []Frame{}})
	if err != nil {
		return "", fmt.Errorf("EnsureChannel encode placeholder: %w", err)
	}

	writes := []writeEntry{
		{relPath: filepath.Join(channelsDir, id, ClientBatchFile), content: placeholder},
		{relPath: filepath.Join(channelsDir, id, ServerBatchFile), content: placeholder},
	}
	if err := c.writeAndPush(ctx, writes, "new channel: "+id); err != nil {
		return "", fmt.Errorf("EnsureChannel: %w", err)
	}
	return id, nil
}

// DeleteChannel removes channels/{channelID}/ and pushes the deletion.
func (c *GitSmartHTTPClient) DeleteChannel(ctx context.Context, channelID string) error {
	c.repoMu.Lock()
	defer c.repoMu.Unlock()

	var pushErr error
	for attempt := 0; attempt < maxPushRetries; attempt++ {
		if attempt > 0 || !speculativePushOK {
			if err := c.fetchAndResetUnderRepoMu(ctx); err != nil {
				pushErr = fmt.Errorf("DeleteChannel sync: %w", err)
				continue
			}
		}

		c.worktreeMu.Lock()
		w, wtErr := c.repo.Worktree()
		if wtErr != nil {
			c.worktreeMu.Unlock()
			return fmt.Errorf("DeleteChannel worktree: %w", wtErr)
		}

		removed := false
		var stageErr error
		for _, fname := range []string{ClientBatchFile, ServerBatchFile} {
			relPath := filepath.Join(channelsDir, channelID, fname)
			if err := os.Remove(filepath.Join(c.workDir, relPath)); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				stageErr = fmt.Errorf("DeleteChannel remove %s: %w", fname, err)
				break
			}
			if _, err := w.Remove(relPath); err != nil {
				stageErr = fmt.Errorf("DeleteChannel stage %s: %w", fname, err)
				break
			}
			removed = true
		}
		_ = os.Remove(filepath.Join(c.workDir, channelsDir, channelID))
		c.worktreeMu.Unlock()

		if stageErr != nil {
			return stageErr
		}
		if !removed {
			return nil
		}

		pushErr = c.commitAndPush(ctx, w, "cleanup channel: "+channelID)
		if pushErr == nil {
			return nil
		}
		if !isGitNonFastForward(pushErr) {
			return pushErr
		}
	}
	return pushErr
}

// ListChannels returns all subdirectories of channels/ from the local working tree.
// Pure filesystem operation — does not touch go-git internals.
func (c *GitSmartHTTPClient) ListChannels(_ context.Context) ([]*ChannelInfo, error) {
	c.worktreeMu.RLock()
	defer c.worktreeMu.RUnlock()

	dir := filepath.Join(c.workDir, channelsDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("ListChannels readdir: %w", err)
	}

	var channels []*ChannelInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, _ := e.Info()
		var updatedAt time.Time
		if info != nil {
			updatedAt = info.ModTime()
		}
		channels = append(channels, &ChannelInfo{
			ID:          e.Name(),
			Description: ChannelDescPrefix,
			UpdatedAt:   updatedAt,
		})
	}
	return channels, nil
}

// Write queues a batch write; concurrent calls within writeBatchWindow share one push.
func (c *GitSmartHTTPClient) Write(ctx context.Context, channelID, filename string, batch *Batch) error {
	data, err := EncodeBatchBytes(batch)
	if err != nil {
		return fmt.Errorf("git Write encode: %w", err)
	}
	req := &writeRequest{
		writes: []writeEntry{{
			relPath: filepath.Join(channelsDir, channelID, filename),
			content: data,
		}},
		done: make(chan error, 1),
	}
	select {
	case c.writeCh <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Read parses channels/{channelID}/{filename} from the local working tree.
// Pure os.ReadFile — never blocked by network ops, only by the brief reset.
func (c *GitSmartHTTPClient) Read(_ context.Context, channelID, filename string) (*Batch, error) {
	c.worktreeMu.RLock()
	defer c.worktreeMu.RUnlock()

	absPath := filepath.Join(c.workDir, channelsDir, channelID, filename)
	data, err := os.ReadFile(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("git Read %s/%s: %w", channelID, filename, err)
	}
	batch, err := DecodeBatchBytes(data)
	if err != nil {
		return nil, fmt.Errorf("git Read %s/%s parse: %w", channelID, filename, err)
	}
	if batch.Seq == 0 && len(batch.Frames) == 0 {
		return nil, nil
	}
	return batch, nil
}

func (c *GitSmartHTTPClient) GetRateLimitInfo() RateLimitInfo {
	return RateLimitInfo{
		Remaining:   99999,
		Limit:       99999,
		ResetAt:     time.Now().Add(1 * time.Hour),
		LastUpdated: time.Now(),
	}
}

func (c *GitSmartHTTPClient) runSyncer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(syncInterval):
		}
		if err := c.fetchAndApply(ctx); err != nil {
			slog.Debug("git transport: background sync failed", "error", err)
		}
	}
}

// fetchAndApply runs one full sync cycle: fetch, then reset the worktree.
func (c *GitSmartHTTPClient) fetchAndApply(ctx context.Context) error {
	c.repoMu.Lock()
	defer c.repoMu.Unlock()
	return c.fetchAndResetUnderRepoMu(ctx)
}

// fetchAndResetUnderRepoMu requires repoMu held by the caller.
func (c *GitSmartHTTPClient) fetchAndResetUnderRepoMu(ctx context.Context) error {
	err := c.repo.FetchContext(ctx, &gogit.FetchOptions{
		RemoteName: "origin",
		Auth:       c.auth,
	})
	if err != nil && !errors.Is(err, gogit.NoErrAlreadyUpToDate) {
		return fmt.Errorf("git fetch: %w", err)
	}

	c.worktreeMu.Lock()
	defer c.worktreeMu.Unlock()
	return c.applyFetchedHead()
}

// applyFetchedHead requires both repoMu and worktreeMu held by the caller.
func (c *GitSmartHTTPClient) applyFetchedHead() error {
	remoteRef := plumbing.NewRemoteReferenceName("origin", c.mainBranch)
	ref, err := c.repo.Reference(remoteRef, true)
	if err != nil {
		return fmt.Errorf("git resolve remote HEAD (%s): %w", remoteRef, err)
	}
	w, err := c.repo.Worktree()
	if err != nil {
		return fmt.Errorf("git worktree: %w", err)
	}
	return w.Reset(&gogit.ResetOptions{Commit: ref.Hash(), Mode: gogit.HardReset})
}

func (c *GitSmartHTTPClient) runBatchWriter(ctx context.Context) {
	for {
		var first *writeRequest
		select {
		case <-ctx.Done():
			return
		case first = <-c.writeCh:
		}

		collected := map[string][]*writeRequest{first.writes[0].relPath: {first}}
		timer := time.NewTimer(writeBatchWindow)
	coalesce:
		for {
			select {
			case req := <-c.writeCh:
				for _, we := range req.writes {
					collected[we.relPath] = append(collected[we.relPath], req)
				}
			case <-timer.C:
				break coalesce
			}
		}
		timer.Stop()

		// Last-write-wins per path, preserving insertion order.
		seen := make(map[string]bool, len(collected))
		var order []writeEntry
		for path, reqs := range collected {
			if seen[path] {
				continue
			}
			seen[path] = true
			last := reqs[len(reqs)-1]
			for _, we := range last.writes {
				if we.relPath == path {
					order = append(order, writeEntry{relPath: path, content: we.content})
					break
				}
			}
		}

		pushErr := c.writeAndPush(ctx, order, "tunnel data")

		// Notify every waiting Write call.
		for _, reqs := range collected {
			for _, req := range reqs {
				req.done <- pushErr
			}
		}
	}
}

// writeAndPush stages files and pushes. First attempt skips the pre-fetch
// (the syncer keeps us at remote HEAD); on non-fast-forward we fetch+reset
// and retry. Caller must NOT already hold repoMu.
func (c *GitSmartHTTPClient) writeAndPush(ctx context.Context, order []writeEntry, msg string) error {
	c.repoMu.Lock()
	defer c.repoMu.Unlock()

	var pushErr error
	for attempt := 0; attempt < maxPushRetries; attempt++ {
		if attempt > 0 || !speculativePushOK {
			if err := c.fetchAndResetUnderRepoMu(ctx); err != nil {
				pushErr = err
				continue
			}
		}

		c.worktreeMu.Lock()
		w, wtErr := c.repo.Worktree()
		if wtErr != nil {
			c.worktreeMu.Unlock()
			return fmt.Errorf("worktree: %w", wtErr)
		}
		var stageErr error
		for _, fw := range order {
			if err := c.writeFileBytes(fw.relPath, fw.content); err != nil {
				stageErr = fmt.Errorf("batch write %s: %w", fw.relPath, err)
				break
			}
			if _, err := w.Add(fw.relPath); err != nil {
				stageErr = fmt.Errorf("batch stage %s: %w", fw.relPath, err)
				break
			}
		}
		c.worktreeMu.Unlock()

		if stageErr != nil {
			return stageErr
		}

		pushErr = c.commitAndPush(ctx, w, msg)
		if pushErr == nil {
			return nil
		}
		if !isGitNonFastForward(pushErr) {
			return pushErr
		}
		slog.Debug("git push non-fast-forward; will fetch+retry", "attempt", attempt+1)
	}
	return pushErr
}

func (c *GitSmartHTTPClient) writeFileBytes(relPath string, content []byte) error {
	absPath := filepath.Join(c.workDir, relPath)
	if err := os.MkdirAll(filepath.Dir(absPath), 0o700); err != nil {
		return err
	}
	return os.WriteFile(absPath, content, 0o600)
}

// commitAndPush requires repoMu. Does not touch the worktree.
func (c *GitSmartHTTPClient) commitAndPush(ctx context.Context, w *gogit.Worktree, msg string) error {
	_, err := w.Commit(msg, &gogit.CommitOptions{
		Author: &object.Signature{
			Name:  "gh-tunnel",
			Email: "tunnel@localhost",
			When:  time.Now(),
		},
		AllowEmptyCommits: false,
	})
	if err != nil {
		s := strings.ToLower(err.Error())
		if strings.Contains(s, "nothing to commit") || strings.Contains(s, "clean") {
			return nil
		}
		return fmt.Errorf("git commit: %w", err)
	}
	pushErr := c.repo.PushContext(ctx, &gogit.PushOptions{Auth: c.auth})
	if errors.Is(pushErr, gogit.NoErrAlreadyUpToDate) {
		return nil
	}
	return pushErr
}

func isEmptyRepoError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "remote repository is empty") ||
		strings.Contains(s, "repository is empty")
}

func seedEmptyRepo(workDir, repoURL string, auth *githttp.BasicAuth) (*gogit.Repository, error) {
	if err := os.RemoveAll(workDir); err != nil {
		return nil, fmt.Errorf("cleanup partial clone: %w", err)
	}
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return nil, fmt.Errorf("recreate workdir: %w", err)
	}
	r, err := gogit.PlainInitWithOptions(workDir, &gogit.PlainInitOptions{
		InitOptions: gogit.InitOptions{
			DefaultBranch: plumbing.NewBranchReferenceName("main"),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("init: %w", err)
	}
	if _, err = r.CreateRemote(&gitconfig.RemoteConfig{
		Name: "origin",
		URLs: []string{repoURL},
	}); err != nil {
		return nil, fmt.Errorf("add remote: %w", err)
	}
	w, err := r.Worktree()
	if err != nil {
		return nil, fmt.Errorf("worktree: %w", err)
	}
	keepPath := filepath.Join(workDir, ".gitkeep")
	if err := os.WriteFile(keepPath, []byte(""), 0o600); err != nil {
		return nil, fmt.Errorf("write .gitkeep: %w", err)
	}
	if _, err := w.Add(".gitkeep"); err != nil {
		return nil, fmt.Errorf("stage .gitkeep: %w", err)
	}
	if _, err = w.Commit("init: tunnel transport repository", &gogit.CommitOptions{
		Author: &object.Signature{
			Name:  "gh-tunnel",
			Email: "tunnel@localhost",
			When:  time.Now(),
		},
	}); err != nil {
		return nil, fmt.Errorf("initial commit: %w", err)
	}
	if err := r.PushContext(context.Background(), &gogit.PushOptions{
		RemoteName: "origin",
		Auth:       auth,
	}); err != nil && !errors.Is(err, gogit.NoErrAlreadyUpToDate) {
		return nil, fmt.Errorf("initial push: %w", err)
	}
	return r, nil
}

func isGitNonFastForward(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "non-fast-forward") ||
		strings.Contains(s, "rejected") ||
		strings.Contains(s, "cannot lock ref")
}
