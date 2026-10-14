package logstore

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AmoabaKelvin/logdeck/internal/config"
	"github.com/AmoabaKelvin/logdeck/internal/logstream"
	"github.com/AmoabaKelvin/logdeck/internal/models"
)

var errCommitBoom = errors.New("config persistence is down")

// newTestController wires a controller over a temp config/database path with
// every LOG_STORE_* environment variable cleared, so each test owns the file
// state.
func newTestController(t *testing.T, containers ...models.ContainerInfo) (*Controller, *fakeHub, *fakeEngine, *config.Manager) {
	t.Helper()
	t.Setenv("CONFIG_PATH", filepath.Join(t.TempDir(), "config.json"))
	for _, key := range []string{
		"LOG_STORE_ENABLED", "LOG_STORE_PER_CONTAINER_MB",
		"LOG_STORE_TOTAL_MB", "LOG_STORE_REMOVED_DAYS",
	} {
		t.Setenv(key, "")
	}

	manager := config.NewManager()
	hub := &fakeHub{}
	engine := newFakeEngine(containers...)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := NewController(ctx, manager, hub, func() Engine { return engine })
	t.Cleanup(c.Shutdown)
	return c, hub, engine, manager
}

func (h *fakeHub) sinkActive() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sink != nil
}

// commitEnabled returns a config commit that persists the enabled flag.
func commitEnabled(manager *config.Manager, enabled bool) func() error {
	return func() error {
		return manager.UpdateLogStore(func(current config.LogStoreConfig) (config.LogStoreConfig, error) {
			value := enabled
			current.Enabled = &value
			return current, nil
		})
	}
}

func liveRecord(ts time.Time, message string) logstream.Record {
	return logstream.Record{
		Host: "local", ContainerID: "aaa", ContainerName: "web",
		Entry: entryAt(ts, "stdout", message),
	}
}

func waitForLines(t *testing.T, store *Store, want int) {
	t.Helper()
	waitFor(t, "stored lines", func() bool { return countLines(t, store) == want })
}

func TestControllerBootstrapDisabledStaysEmpty(t *testing.T) {
	c, hub, _, manager := newTestController(t)
	if err := manager.UpdateLogStore(func(current config.LogStoreConfig) (config.LogStoreConfig, error) {
		off := false
		current.Enabled = &off
		return current, nil
	}); err != nil {
		t.Fatalf("disable in config: %v", err)
	}

	c.Bootstrap()

	if store := c.CurrentStore(); store != nil {
		t.Fatalf("CurrentStore = %v, want nil with persistence disabled", store)
	}
	if hub.sinkActive() {
		t.Fatal("hub subscription exists with persistence disabled")
	}
}

func TestControllerDisableEnableRoundTrip(t *testing.T) {
	info := containerInfo("local", "aaa", "web", baseTime)
	c, hub, engine, manager := newTestController(t, info)
	c.Bootstrap()

	first := c.CurrentStore()
	if first == nil || !hub.sinkActive() {
		t.Fatal("Bootstrap did not start the pipeline")
	}
	waitFor(t, "initial backfill", func() bool { return engine.calls("aaa") == 1 })

	// Live lines committed before the toggle must survive it.
	for i := 1; i <= 3; i++ {
		hub.emit(liveRecord(baseTime.Add(time.Duration(i)*time.Second), "live"))
	}
	waitForLines(t, first, 3)

	// Disable: input cut immediately, in-flight writes flushed, resources
	// stopped, config persisted off.
	if err := c.Disable(commitEnabled(manager, false)); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	if c.CurrentStore() != nil {
		t.Fatal("CurrentStore non-nil after Disable")
	}
	if hub.sinkActive() {
		t.Fatal("hub subscription still active after Disable")
	}
	if manager.LogStore().Enabled {
		t.Fatal("config still enabled after Disable committed")
	}

	// Re-enable: same database file, hub reattached, backfill resumes from the
	// persisted watermark with the overlap window.
	if err := c.Enable(context.Background(), commitEnabled(manager, true)); err != nil {
		t.Fatalf("Enable: %v", err)
	}
	second := c.CurrentStore()
	if second == nil || second == first {
		t.Fatalf("Enable did not publish a fresh store: %v", second)
	}
	if !hub.sinkActive() {
		t.Fatal("hub not re-subscribed after Enable")
	}
	if !manager.LogStore().Enabled {
		t.Fatal("config disabled after Enable committed")
	}

	waitFor(t, "gap backfill after re-enable", func() bool { return engine.calls("aaa") == 2 })
	wantSince := time.Unix(0, baseTime.Add(3*time.Second).UnixNano()).Add(-backfillOverlap).
		UTC().Format(time.RFC3339Nano)
	if got := engine.opts("aaa").Since; got != wantSince {
		t.Fatalf("backfill Since = %s, want watermark minus overlap %s", got, wantSince)
	}

	// Boundary discipline: the line already persisted at the edge is delivered
	// again by the live attachment, and a genuinely new line lands once.
	hub.emit(liveRecord(baseTime.Add(3*time.Second), "live"))
	hub.emit(liveRecord(baseTime.Add(4*time.Second), "live"))
	waitForLines(t, second, 4)
}

func TestControllerEnableFailureTearsPreparedStoreDown(t *testing.T) {
	c, hub, _, manager := newTestController(t)
	// Start from a disabled config, as a service booted with persistence off.
	if err := manager.UpdateLogStore(func(current config.LogStoreConfig) (config.LogStoreConfig, error) {
		off := false
		current.Enabled = &off
		return current, nil
	}); err != nil {
		t.Fatalf("disable: %v", err)
	}

	// The pipeline is prepared before the commit; a failing commit must discard
	// it completely instead of reporting a hollow success.
	err := c.Enable(context.Background(), func() error { return errCommitBoom })
	if !errors.Is(err, errCommitBoom) {
		t.Fatalf("Enable = %v, want the commit error", err)
	}
	if c.CurrentStore() != nil || hub.sinkActive() {
		t.Fatal("failed Enable leaked a running store or subscription")
	}
	if manager.LogStore().Enabled {
		t.Fatal("config flipped despite the failed commit")
	}

	// The same request is retryable once the fault is gone.
	if err := c.Enable(context.Background(), commitEnabled(manager, true)); err != nil {
		t.Fatalf("retry Enable: %v", err)
	}
	if c.CurrentStore() == nil || !hub.sinkActive() {
		t.Fatal("retry did not start the pipeline")
	}
}

func TestControllerDisableCommitFailureReopens(t *testing.T) {
	c, hub, _, manager := newTestController(t)
	c.Bootstrap()
	before := c.CurrentStore()

	err := c.Disable(func() error { return errCommitBoom })
	if !errors.Is(err, errCommitBoom) {
		t.Fatalf("Disable = %v, want the commit error surfaced", err)
	}

	// Config stayed enabled, so the runtime converges back to a fresh store.
	after := c.CurrentStore()
	if after == nil || after == before {
		t.Fatalf("runtime not restored after failed Disable: %v", after)
	}
	if !hub.sinkActive() {
		t.Fatal("restored store is not subscribed")
	}
	if !manager.LogStore().Enabled {
		t.Fatal("config flipped despite the failed commit")
	}

	hub.emit(liveRecord(baseTime.Add(time.Second), "live"))
	waitForLines(t, after, 1)
}

func TestControllerDuplicateRequestsAreNoop(t *testing.T) {
	c, _, _, manager := newTestController(t)
	c.Bootstrap()
	running := c.CurrentStore()

	// Enable while already enabled commits the config but allocates no store.
	called := 0
	commit := func() error { called++; return nil }
	if err := c.Enable(context.Background(), commit); err != nil {
		t.Fatalf("duplicate Enable: %v", err)
	}
	if c.CurrentStore() != running {
		t.Fatal("duplicate Enable replaced the running store")
	}
	if called != 1 {
		t.Fatalf("commit called %d times, want 1", called)
	}

	if err := c.Disable(commitEnabled(manager, false)); err != nil {
		t.Fatalf("Disable: %v", err)
	}
	// Disable while already disabled only commits (used here with a failing
	// commit to prove no store is touched on the runtime side).
	if err := c.Disable(func() error { return errCommitBoom }); err == nil {
		t.Fatal("expected the commit error to surface on a duplicate disable")
	}
	if c.CurrentStore() != nil {
		t.Fatal("duplicate Disable started a store")
	}
}

func TestControllerConcurrentTogglesSettle(t *testing.T) {
	c, hub, engine, manager := newTestController(t)
	c.Bootstrap()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(even bool) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				if even {
					_ = c.Disable(commitEnabled(manager, false))
					_ = c.Enable(context.Background(), commitEnabled(manager, true))
				} else {
					_ = c.Enable(context.Background(), commitEnabled(manager, true))
					_ = c.Disable(commitEnabled(manager, false))
				}
			}
		}(i%2 == 0)
	}
	wg.Wait()

	if err := c.Enable(context.Background(), commitEnabled(manager, true)); err != nil {
		t.Fatalf("settle Enable: %v", err)
	}
	store := c.CurrentStore()
	if store == nil || !hub.sinkActive() {
		t.Fatal("pipeline not running after the toggle storm")
	}
	_ = engine
	hub.emit(liveRecord(baseTime.Add(10*time.Second), "settled"))
	waitForLines(t, store, 1)
}

func TestControllerToggleAfterShutdownRejected(t *testing.T) {
	c, _, _, _ := newTestController(t)
	c.Bootstrap()
	c.Shutdown()

	commits := 0
	noCommit := func() error { commits++; return nil }

	if err := c.Enable(context.Background(), noCommit); !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("Enable after Shutdown = %v, want ErrStoreUnavailable", err)
	}
	if err := c.Disable(noCommit); !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("Disable after Shutdown = %v, want ErrStoreUnavailable", err)
	}
	if commits != 0 {
		t.Fatalf("commit ran %d times after shutdown", commits)
	}
	if c.CurrentStore() != nil {
		t.Fatal("store published after shutdown")
	}

	// Shutdown is idempotent.
	c.Shutdown()
}
