package alerts

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AmoabaKelvin/logdeck/internal/config"
	"github.com/AmoabaKelvin/logdeck/internal/models"
)

// gateHandler blocks every request until release or the request context
// aborts (shutdown cancellation), so a test server never keeps Close waiting.
func gateHandler(t *testing.T, hits *atomic.Int32, proceed <-chan struct{}) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-proceed:
		case <-r.Context().Done():
		}
	}
}

// countHandler replies code and counts hits.
func countHandler(hits *atomic.Int32, code int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(code)
	}
}

// twoPatternRules returns two threshold-1 log rules with distinct patterns so
// two alerts can be fired back to back without hitting cooldown windows.
func twoPatternRules() []config.AlertRule {
	return []config.AlertRule{
		{ID: "r1", Name: "one", Enabled: true, Type: "log", Pattern: "boom1", Threshold: 1},
		{ID: "r2", Name: "two", Enabled: true, Type: "log", Pattern: "boom2", Threshold: 1},
	}
}

// A channel edited after an alert triggered but before it is dispatched must
// not redirect the queued alert: the trigger-time snapshot pins the original
// target (and its parameters), and history records the snapshot's stable
// channel ID.
func TestQueuedAlertUsesSnapshotWhenChannelEdited(t *testing.T) {
	proceed := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	var blockHits, newHits atomic.Int32
	blockSrv := httptest.NewServer(gateHandler(t, &blockHits, proceed))
	defer blockSrv.Close()
	defer release()
	newSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newHits.Add(1)
	}))
	defer newSrv.Close()

	rules := twoPatternRules()
	te := startTestEngine(t, rules...)
	te.conf.set(config.AlertsConfig{Channels: webhookChannels(blockSrv.URL), Rules: rules})
	waitFor(t, "subscriptions", func() bool { return te.hub.liveCount() == 2 })

	// Alert 1 occupies the dispatcher; its snapshot delivery hangs on blockSrv.
	te.hub.emit(record(models.LogLevelError, "boom1"))
	waitFor(t, "first alert in history", func() bool { return len(te.e.History(0)) == 1 })

	// Alert 2 is captured against the same channel revision and waits queued.
	te.hub.emit(record(models.LogLevelError, "boom2"))
	waitFor(t, "second alert queued", func() bool { return len(te.e.dispatchCh) == 1 })

	// Same stable ID, brand-new address, after both trigger points.
	te.conf.set(config.AlertsConfig{
		Channels: []config.AlertChannel{{ID: "c1", Type: "webhook", Enabled: true, URL: newSrv.URL + "/new-secret"}},
		Rules:    rules,
	})

	release()
	waitFor(t, "both delivered", func() bool {
		h := te.e.History(0)
		return len(h) == 2 && h[0].Delivery != nil && h[1].Delivery != nil
	})

	if got := newHits.Load(); got != 0 {
		t.Fatalf("edited channel received %d in-flight/queued requests, want 0", got)
	}
	if got := blockHits.Load(); got != 2 {
		t.Fatalf("original target received %d requests, want 2 (one per alert snapshot)", got)
	}
	for _, a := range te.e.History(0) {
		if len(a.Channels) != 1 || a.Channels[0].ChannelID != "c1" || a.Channels[0].ChannelType != "webhook" || a.Channels[0].Status != "ok" {
			t.Fatalf("alert %s per-channel results = %+v, want one ok result for snapshot channel c1", a.ID, a.Channels)
		}
	}

	// The per-channel result must survive history persistence without leaking
	// any target or secret.
	te.e.hist.flushIfDirty()
	data, err := os.ReadFile(te.path)
	if err != nil {
		t.Fatalf("read flushed history: %v", err)
	}
	s := string(data)
	if !strings.Contains(s, `"channelId": "c1"`) || !strings.Contains(s, `"channelType": "webhook"`) {
		t.Fatalf("flushed history missing per-channel results: %s", s)
	}
	if strings.Contains(s, "new-secret") || strings.Contains(s, blockSrv.URL) {
		t.Fatalf("flushed history leaked channel target/secret: %s", s)
	}
}

// Disabling or deleting a channel after the trigger point must not suppress
// the queued alert: it still fans out to every snapshot channel and keeps
// their per-channel outcomes.
func TestQueuedAlertDeliveredAfterChannelsDeleted(t *testing.T) {
	proceed := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	var blockHits, targetHits atomic.Int32
	blockSrv := httptest.NewServer(gateHandler(t, &blockHits, proceed))
	defer blockSrv.Close()
	defer release()
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
	}))
	defer targetSrv.Close()

	rules := twoPatternRules()
	te := startTestEngine(t, rules...)
	te.conf.set(config.AlertsConfig{
		Channels: []config.AlertChannel{
			{ID: "c1", Type: "webhook", Name: "blocker", Enabled: true, URL: blockSrv.URL},
			{ID: "c2", Type: "webhook", Name: "target", Enabled: true, URL: targetSrv.URL},
		},
		Rules: rules,
	})
	waitFor(t, "subscriptions", func() bool { return te.hub.liveCount() == 2 })

	te.hub.emit(record(models.LogLevelError, "boom1"))
	waitFor(t, "first alert in history", func() bool { return len(te.e.History(0)) == 1 })
	waitFor(t, "first alert reaches the non-blocking channel", func() bool { return targetHits.Load() == 1 })

	te.hub.emit(record(models.LogLevelError, "boom2"))
	waitFor(t, "second alert queued", func() bool { return len(te.e.dispatchCh) == 1 })

	// Operator deletes all channels while the second alert sits queued.
	te.conf.set(config.AlertsConfig{Channels: nil, Rules: rules})

	release()
	waitFor(t, "both delivered", func() bool {
		h := te.e.History(0)
		return len(h) == 2 && h[0].Delivery != nil && h[1].Delivery != nil
	})

	if got := targetHits.Load(); got != 2 {
		t.Fatalf("deleted channel target received %d requests, want 2", got)
	}
	if got := blockHits.Load(); got != 2 {
		t.Fatalf("deleted blocking channel received %d requests, want 2", got)
	}
	for _, a := range te.e.History(0) {
		ids := map[string]bool{}
		if len(a.Channels) != 2 {
			t.Fatalf("alert %s channels = %+v, want both snapshot channels retained", a.ID, a.Channels)
		}
		for _, c := range a.Channels {
			ids[c.ChannelID] = true
			if c.Status != "ok" {
				t.Fatalf("alert %s channel %s = %+v, want ok", a.ID, c.ChannelID, c)
			}
		}
		if !ids["c1"] || !ids["c2"] {
			t.Fatalf("alert %s per-channel ids = %v, want c1 and c2", a.ID, ids)
		}
	}
}

// One failing channel must not prevent delivery to the others, and history
// keeps the outcome of each channel independently.
func TestPerChannelFailureIsolation(t *testing.T) {
	var goodHits, badHits atomic.Int32
	goodSrv := httptest.NewServer(countHandler(&goodHits, http.StatusOK))
	defer goodSrv.Close()
	badSrv := httptest.NewServer(countHandler(&badHits, http.StatusInternalServerError))
	defer badSrv.Close()

	te := startTestEngine(t, config.AlertRule{ID: "r1", Name: "boom", Enabled: true, Type: "log", Pattern: "boom", Threshold: 1})
	te.e.notif.retryDelay = 0 // retry immediately so the test stays fast
	te.conf.set(config.AlertsConfig{
		Channels: []config.AlertChannel{
			{ID: "c1", Type: "webhook", Name: "bad", Enabled: true, URL: badSrv.URL},
			{ID: "c2", Type: "webhook", Name: "good", Enabled: true, URL: goodSrv.URL},
		},
		Rules: te.conf.get().Rules,
	})
	waitFor(t, "subscription", func() bool { return te.hub.liveCount() == 1 })

	te.hub.emit(record(models.LogLevelError, "boom"))
	var entry models.Alert
	waitFor(t, "delivery recorded", func() bool {
		h := te.e.History(0)
		if len(h) != 1 || h[0].Delivery == nil {
			return false
		}
		entry = h[0]
		return true
	})

	if got := goodHits.Load(); got != 1 {
		t.Fatalf("healthy channel hits = %d, want 1 (a bad peer must not starve it)", got)
	}
	if got := badHits.Load(); got != 2 {
		t.Fatalf("failing channel hits = %d, want 2 (one immediate retry on 5xx)", got)
	}
	if entry.Delivery.Status != "failed" {
		t.Fatalf("summary = %+v, want failed", entry.Delivery)
	}
	byID := map[string]models.ChannelDelivery{}
	for _, c := range entry.Channels {
		byID[c.ChannelID] = c
	}
	bad, ok1 := byID["c1"]
	good, ok2 := byID["c2"]
	if !ok1 || !ok2 {
		t.Fatalf("per-channel results = %+v, want c1 and c2", entry.Channels)
	}
	if bad.Status != "failed" || bad.HTTPStatus != http.StatusInternalServerError || bad.Error == "" {
		t.Fatalf("bad channel result = %+v, want failed/500 with error", bad)
	}
	if good.Status != "ok" || good.HTTPStatus != http.StatusOK {
		t.Fatalf("good channel result = %+v, want ok/200", good)
	}
}

// The post-failure retry must reuse the trigger-time request (original target
// and parameters) even if the channel was edited during the retry wait.
func TestRetryHitsOriginalTargetAfterConfigEdit(t *testing.T) {
	var oldHits, newHits atomic.Int32
	oldSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if oldHits.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))
	defer oldSrv.Close()
	newSrv := httptest.NewServer(countHandler(&newHits, http.StatusOK))
	defer newSrv.Close()

	te := startTestEngine(t, config.AlertRule{ID: "r1", Name: "boom", Enabled: true, Type: "log", Pattern: "boom", Threshold: 1})
	te.e.notif.retryDelay = 100 * time.Millisecond
	te.conf.set(config.AlertsConfig{
		Channels: []config.AlertChannel{{ID: "c1", Type: "webhook", Enabled: true, URL: oldSrv.URL + "/old-target"}},
		Rules:    te.conf.get().Rules,
	})
	waitFor(t, "subscription", func() bool { return te.hub.liveCount() == 1 })

	te.hub.emit(record(models.LogLevelError, "boom"))
	waitFor(t, "first attempt failed", func() bool { return oldHits.Load() == 1 })

	// Edit the channel while the retry is waiting.
	te.conf.set(config.AlertsConfig{
		Channels: []config.AlertChannel{{ID: "c1", Type: "webhook", Enabled: true, URL: newSrv.URL}},
		Rules:    te.conf.get().Rules,
	})

	waitFor(t, "delivery recorded", func() bool {
		h := te.e.History(0)
		return len(h) == 1 && h[0].Delivery != nil
	})
	if got := oldHits.Load(); got != 2 {
		t.Fatalf("original target hits = %d, want 2 (initial attempt + retry)", got)
	}
	if got := newHits.Load(); got != 0 {
		t.Fatalf("retry followed the edited target: new channel hits = %d, want 0", got)
	}
	if d := te.e.History(0)[0].Delivery; d.Status != "ok" {
		t.Fatalf("delivery = %+v, want ok after retrying the original target", d)
	}
}

// When the shutdown drain budget expires, the in-flight alert and the queued
// alert both reach a deterministic terminal state derived solely from their
// snapshots; the dispatcher must not read (and follow) live configuration.
func TestShutdownDrainTerminalStatesUseSnapshot(t *testing.T) {
	proceed := make(chan struct{}) // not released during the test: the drain
	// budget aborts the request client-side; it is closed on teardown so the
	// gate handler can return before the test server closes.
	var blockHits atomic.Int32
	blockSrv := httptest.NewServer(gateHandler(t, &blockHits, proceed))
	defer blockSrv.Close()
	defer close(proceed)
	var goodHits atomic.Int32
	goodSrv := httptest.NewServer(countHandler(&goodHits, http.StatusOK))
	defer goodSrv.Close()
	var swappedHits atomic.Int32
	swappedSrv := httptest.NewServer(countHandler(&swappedHits, http.StatusOK))
	defer swappedSrv.Close()

	rules := twoPatternRules()
	te := &testEngine{
		hub:    &fakeHub{},
		events: newFakeEvents(),
		conf:   &fakeConf{cfg: config.AlertsConfig{Rules: rules}},
		clock:  &fakeClock{t: t0},
		path:   filepath.Join(t.TempDir(), "alerts-history.json"),
	}
	te.e = newEngine(te.hub, func() eventClient { return te.events }, te.conf.get, te.path)
	te.e.now = te.clock.now
	te.e.shutdownDrain = 250 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	te.cancel = cancel
	te.e.Start(ctx)
	t.Cleanup(func() {
		cancel()
		te.e.Wait()
	})

	te.conf.set(config.AlertsConfig{
		Channels: []config.AlertChannel{
			{ID: "c1", Type: "webhook", Name: "block", Enabled: true, URL: blockSrv.URL},
			{ID: "c2", Type: "webhook", Name: "good", Enabled: true, URL: goodSrv.URL},
		},
		Rules: rules,
	})
	waitFor(t, "subscriptions", func() bool { return te.hub.liveCount() == 2 })

	te.hub.emit(record(models.LogLevelError, "boom1"))
	waitFor(t, "first alert in history", func() bool { return len(te.e.History(0)) == 1 })
	te.hub.emit(record(models.LogLevelError, "boom2"))
	waitFor(t, "second alert queued", func() bool { return len(te.e.dispatchCh) == 1 })

	cancel()
	// Swap live configuration during the drain: no delivery may follow it.
	te.conf.set(config.AlertsConfig{
		Channels: []config.AlertChannel{{ID: "c9", Type: "webhook", Enabled: true, URL: swappedSrv.URL}},
		Rules:    rules,
	})

	done := make(chan struct{})
	go func() {
		te.e.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not return promptly after the drain budget")
	}

	h := te.e.History(0)
	if len(h) != 2 {
		t.Fatalf("history = %d entries, want 2", len(h))
	}
	for _, a := range h {
		if a.Delivery == nil || a.Delivery.Status != "failed" {
			t.Fatalf("alert %s delivery = %+v, want a failed terminal state", a.ID, a.Delivery)
		}
	}
	// Newest entry is the queued alert: every snapshot channel gets a
	// deterministic "shutdown" result without any delivery attempt.
	queued := h[0]
	if len(queued.Channels) != 2 {
		t.Fatalf("queued alert channels = %+v, want 2 snapshot channels", queued.Channels)
	}
	for _, c := range queued.Channels {
		if c.Status != "failed" || c.Error != "shutdown" {
			t.Fatalf("queued alert channel result = %+v, want failed/shutdown", c)
		}
	}
	// In-flight entry: the healthy channel already succeeded; the blocked one
	// was aborted by the drain and is failed. Neither followed the swap.
	inFlight := h[1]
	byID := map[string]models.ChannelDelivery{}
	for _, c := range inFlight.Channels {
		byID[c.ChannelID] = c
	}
	if c := byID["c2"]; c.Status != "ok" {
		t.Fatalf("in-flight good channel = %+v, want ok before the drain", c)
	}
	if c := byID["c1"]; c.Status != "failed" || c.Error == "" {
		t.Fatalf("in-flight blocked channel = %+v, want failed with an abort error", c)
	}
	if got := swappedHits.Load(); got != 0 {
		t.Fatalf("drain followed live config: swapped channel hits = %d, want 0", got)
	}

	// The terminal states reach the persisted history file.
	data, err := os.ReadFile(te.path)
	if err != nil {
		t.Fatalf("history not flushed: %v", err)
	}
	s := string(data)
	for _, want := range []string{`"error": "shutdown"`, `"channelId": "c1"`, `"channelId": "c2"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("flushed history missing %q in terminal per-channel results: %s", want, s)
		}
	}
}

// Legacy history files (delivery summary, no per-channel results) keep
// loading, and old entries survive a new flush unchanged in shape.
func TestHistoryLoadsLegacyEntriesWithoutChannelResults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts-history.json")
	legacy := `[
  {
    "id": "abcd1234",
    "ruleId": "r1",
    "ruleName": "old",
    "type": "log",
    "host": "local",
    "containerId": "",
    "containerName": "web",
    "reason": "boom",
    "count": 1,
    "suppressed": 0,
    "firedAt": "2026-07-10T12:00:00Z",
    "delivery": {"status": "failed", "httpStatus": 500, "error": "channel returned 500 Internal Server Error"}
  }
]`
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatalf("write legacy history: %v", err)
	}
	h := newHistory(path, historyCap)
	h.load()
	entries := h.list(0)
	if len(entries) != 1 || entries[0].Delivery == nil || entries[0].Delivery.Status != "failed" {
		t.Fatalf("legacy entry = %+v, want delivery summary preserved", entries)
	}
	if entries[0].Channels != nil {
		t.Fatalf("legacy channels = %+v, want nil", entries[0].Channels)
	}

	// A new result written on top keeps both shapes side by side and
	// round-trips through disk.
	h.setDelivery("abcd1234", models.DeliveryResult{Status: "ok"}, []models.ChannelDelivery{
		{ChannelID: "c1", ChannelType: "webhook", Status: "ok", HTTPStatus: 200},
	})
	h.flushIfDirty()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read flushed history: %v", err)
	}
	var onDisk []models.Alert
	if err := json.Unmarshal(data, &onDisk); err != nil || len(onDisk) != 1 {
		t.Fatalf("flushed history = %s (err %v)", data, err)
	}
	if len(onDisk[0].Channels) != 1 || onDisk[0].Channels[0].ChannelID != "c1" {
		t.Fatalf("flushed per-channel results = %+v", onDisk[0].Channels)
	}
}
