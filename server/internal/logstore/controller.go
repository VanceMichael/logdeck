package logstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/AmoabaKelvin/logdeck/internal/config"
)

// ErrStoreUnavailable marks a toggle failure where persistence could not be
// made operational: the database would not open (or the process is shutting
// down). The settings API answers 503 with this error instead of masking the
// degradation with a 200; the same request succeeds once retried after the
// underlying fault is gone.
var ErrStoreUnavailable = errors.New("log persistence is unavailable")

// Controller owns the optional Store for the lifetime of a process and drives
// its start/stop lifecycle when the "logStore.enabled" setting is toggled at
// runtime. The Store itself is a one-shot pipeline (channels, WaitGroups, and
// its hub subscription are not reusable), so every transition creates a fresh
// Store and the previous one is joined and closed exactly once.
//
// Invariants:
//
//   - Transitions are serialized by one mutex: concurrent or duplicate toggle
//     requests run one at a time, and a request that matches the running state
//     allocates no Store, goroutine, subscription, or database handle.
//   - On disable the running Store is unpublished and its lifecycle context is
//     cancelled in the same locked step, after which its hub sink is never
//     invoked again; Disable only returns once in-flight writes are flushed and
//     the sync/backfill/janitor goroutines and both SQLite handles are stopped.
//   - On enable the original database is reopened and the hub subscription and
//     engine backfill attached before the change is reported. The persisted
//     watermarks and the writer's insert-time dedup then heal the whole
//     disabled interval, including its boundaries, without repeats or gaps.
//   - The config commit is interleaved with the runtime transition, so a
//     failed open, teardown, or config write cannot leave the persisted value
//     and the running state disagreeing. Every failed path stays retryable.
type Controller struct {
	parent  context.Context
	manager *config.Manager
	hub     Hub
	// source yields the current engine; read indirectly on every sync pass so
	// the Store keeps working across hot-swapped Docker clients.
	source func() Engine

	mu      sync.Mutex
	running *Store
	cancel  context.CancelFunc
	closed  bool
}

// NewController builds the runtime owner. parent bounds every Store the
// controller starts: when it is cancelled the active Store drains on its own,
// and Shutdown then joins and closes it.
func NewController(parent context.Context, manager *config.Manager, hub Hub, source func() Engine) *Controller {
	return &Controller{
		parent:  parent,
		manager: manager,
		hub:     hub,
		source:  source,
	}
}

// CurrentStore returns the running store, or nil when persistence is off. The
// pointer is per-request: a concurrent toggle may stop and close it, and
// database/sql makes using a closing pool safe (the call returns an error the
// API maps to "disabled") rather than racing.
func (c *Controller) CurrentStore() *Store {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// Bootstrap starts the Store when the resolved configuration has persistence
// enabled. It keeps OpenFromConfig's tolerant startup contract: a disabled
// config, or an unusable database, logs a warning and leaves the controller
// empty so the rest of the server runs without stored logs.
func (c *Controller) Bootstrap() {
	store := OpenFromConfig(c.manager)
	if store == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.parent.Err() != nil {
		if err := store.Close(); err != nil {
			log.Printf("logstore: closing the unused store failed: %v", err)
		}
		return
	}

	ctx, cancel := context.WithCancel(c.parent)
	store.start(ctx, c.hub, c.source)
	c.running, c.cancel = store, cancel
}

// Enable opens the database and starts the ingestion pipeline BEFORE running
// commit, so the prepared pipeline can be torn back down when the config write
// fails. The new Store is only published once commit succeeded, hence every
// successful response already sees status, query endpoints, and MCP enabled,
// and every failed response leaves config and runtime both disabled.
//
// Re-enabling reopens the same file: the new Store snapshots the persisted
// per-generation watermarks and the sync loop backfills the disabled interval
// with an overlap, while the writer deduplicates overlapping reads.
func (c *Controller) Enable(ctx context.Context, commit func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed || c.parent.Err() != nil || ctx.Err() != nil {
		return fmt.Errorf("%w: server is shutting down", ErrStoreUnavailable)
	}
	if c.running != nil {
		// Idempotent duplicate: the runtime already matches the request, so the
		// config commit is the only work left (caps may accompany the toggle).
		return commit()
	}

	store, cancel, err := c.openStartLocked()
	if err != nil {
		return err
	}

	if commitErr := commit(); commitErr != nil {
		// Config stayed disabled: discard the prepared pipeline before it can
		// be observed, and leave no goroutine, subscription, or handle behind.
		cancel()
		store.Wait()
		if closeErr := store.Close(); closeErr != nil {
			log.Printf("logstore: closing the store after a failed enable failed: %v", closeErr)
		}
		return commitErr
	}

	c.running, c.cancel = store, cancel
	log.Printf("Log persistence is ENABLED (%s)", store.path)
	return nil
}

// Disable unpublishes the Store and blocks new persistence input in one locked
// step: the Store's shutdown coordinator drops the hub subscription as soon as
// it observes the cancellation, after which the sink is never called again.
// The disabled config is committed next (while the pipeline drains), so
// settings and status can never be observed disagreeing; Disable only returns
// after in-flight backfills and the final writer batch have completed and both
// SQLite handles are closed. The on-disk history itself is untouched.
//
// commit runs while the transition lock is held. When it fails the in-memory
// config has already rolled back to enabled, so Disable converges the runtime
// back to it by reopening a fresh Store; the response still reports the
// failure and the operation can be retried.
func (c *Controller) Disable(commit func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("%w: server is shutting down", ErrStoreUnavailable)
	}

	old, stop := c.running, c.cancel
	if old == nil {
		// Runtime is already disabled; just make the config agree.
		return commit()
	}

	// Immediate, atomic user-visible flip + input cut-off.
	c.running, c.cancel = nil, nil
	stop()

	// Persist the disabled intent while the pipeline drains in the background:
	// from here on status AND settings report disabled, so an observer can never
	// catch the two disagreeing. The database file itself stays untouched.
	if err := commit(); err != nil {
		// The persisted config stayed enabled: let the cancelled Store finish
		// draining, release its handles, and reopen a fresh Store so runtime and
		// config converge.
		old.Wait()
		closeErr := old.Close()
		store, cancel, reopenErr := c.openStartLocked()
		if reopenErr != nil {
			if closeErr != nil {
				log.Printf("logstore: closing the old store after a failed disable failed: %v", closeErr)
			}
			return errors.Join(err, reopenErr)
		}
		if closeErr != nil {
			log.Printf("logstore: closing the old store after a failed disable failed: %v", closeErr)
		}
		c.running, c.cancel = store, cancel
		log.Printf("Log persistence is ENABLED (%s)", store.path)
		return err
	}

	// Join the sync/backfill/janitor goroutines and let the writer flush its
	// final batch, then release both SQLite handles. A close error cannot split
	// config and state (the Store is already unpublished and drained), so it is
	// logged rather than surfaced.
	old.Wait()
	if err := old.Close(); err != nil {
		log.Printf("logstore: closing the disabled store failed: %v", err)
	}
	log.Println("Log persistence is DISABLED")
	return nil
}

// Shutdown deterministically stops the active Store at process exit. It is
// idempotent and a no-op when persistence is off or was toggled off earlier;
// a parent-context cancellation has usually started the drain already, and
// Shutdown still joins it and closes the handles exactly once.
func (c *Controller) Shutdown() {
	c.mu.Lock()
	c.closed = true
	store, stop := c.running, c.cancel
	c.running, c.cancel = nil, nil
	c.mu.Unlock()

	if stop == nil {
		return
	}
	stop()
	store.Wait()
	if err := store.Close(); err != nil {
		log.Printf("Closing the log store failed: %v", err)
	}
}

// openStartLocked opens a fresh Store against the configured database path and
// starts its pipeline. The caller must hold c.mu. The Store is returned
// unpublished: callers publish it only after their config commit succeeds.
func (c *Controller) openStartLocked() (*Store, context.CancelFunc, error) {
	if c.closed || c.parent.Err() != nil {
		return nil, nil, fmt.Errorf("%w: server is shutting down", ErrStoreUnavailable)
	}

	store, err := Open(DBPath(c.manager.ConfigFilePath()), c.manager.LogStore)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
	}

	ctx, cancel := context.WithCancel(c.parent)
	store.start(ctx, c.hub, c.source)
	return store, cancel, nil
}
