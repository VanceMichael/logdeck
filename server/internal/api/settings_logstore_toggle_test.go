package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/AmoabaKelvin/logdeck/internal/config"
	"github.com/AmoabaKelvin/logdeck/internal/docker"
	"github.com/AmoabaKelvin/logdeck/internal/logstore"
	"github.com/AmoabaKelvin/logdeck/internal/logstream"
	"github.com/AmoabaKelvin/logdeck/internal/models"
	"github.com/AmoabaKelvin/logdeck/internal/services"
)

// toggleHub is the minimal Hub the controller accepts; it only records whether
// the store's subscription is currently attached.
type toggleHub struct{ active atomic.Bool }

func (h *toggleHub) Subscribe(_ logstream.ContainerSpec, _ models.LogOptions, _ func(logstream.Record)) func() {
	h.active.Store(true)
	return func() { h.active.Store(false) }
}

// toggleEngine lists nothing and tails nothing: lifecycle transitions are what
// these tests exercise, not ingestion.
type toggleEngine struct{}

func (toggleEngine) ListContainersAllHosts(context.Context) (map[string][]models.ContainerInfo, []docker.HostError, error) {
	return map[string][]models.ContainerInfo{"local": {}}, nil, nil
}

func (toggleEngine) TailContainerLogs(context.Context, string, string, models.LogOptions, func(models.LogEntry)) error {
	return nil
}

// newToggleRouter wires a router to a real log store controller over a temp
// config path. The controller starts empty (no Bootstrap), like a service that
// booted with persistence disabled.
func newToggleRouter(t *testing.T, configPath string) (http.Handler, *config.Manager, *logstore.Controller, *toggleHub) {
	t.Helper()
	for _, key := range []string{
		"JWT_SECRET", "ADMIN_USERNAME", "ADMIN_PASSWORD", "ADMIN_PASSWORD_SALT",
		"DOCKER_HOSTS", "COOLIFY_CONFIGS", "READONLY_MODE", "CORS_ALLOWED_ORIGINS",
		"LOG_STORE_ENABLED", "LOG_STORE_PER_CONTAINER_MB", "LOG_STORE_TOTAL_MB",
		"LOG_STORE_REMOVED_DAYS",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("CONFIG_PATH", configPath)

	manager := config.NewManager()
	registry := services.NewRegistry(nil, nil, nil, manager.Config())
	hub := &toggleHub{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	engine := toggleEngine{}
	ctl := logstore.NewController(ctx, manager, hub, func() logstore.Engine { return engine })
	t.Cleanup(ctl.Shutdown)
	return NewRouter(registry, manager, nil, ctl, "test"), manager, ctl, hub
}

func doRequest(t *testing.T, router http.Handler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, target, payload)
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestUpdateLogStorageTogglesRuntime(t *testing.T) {
	router, manager, ctl, hub := newToggleRouter(t, filepath.Join(t.TempDir(), "config.json"))

	if ctl.CurrentStore() != nil {
		t.Fatal("controller should start empty")
	}

	// Enable from settings: the response must mean the store is actually open.
	w := doRequest(t, router, http.MethodPut, "/api/v1/settings/log-storage", `{"enabled":true}`)
	if w.Code != http.StatusOK {
		t.Fatalf("enable = %d, want 200: %s", w.Code, w.Body.String())
	}
	if ctl.CurrentStore() == nil || !hub.active.Load() {
		t.Fatal("enabled response with no running, subscribed store")
	}
	if !manager.LogStore().Enabled {
		t.Fatal("config not persisted enabled")
	}

	w = doRequest(t, router, http.MethodGet, "/api/v1/history/status", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"enabled":true`) {
		t.Fatalf("history status = %d %s, want enabled", w.Code, w.Body.String())
	}

	// Disable: history state and every query entry go unavailable at once.
	w = doRequest(t, router, http.MethodPut, "/api/v1/settings/log-storage", `{"enabled":false}`)
	if w.Code != http.StatusOK {
		t.Fatalf("disable = %d, want 200: %s", w.Code, w.Body.String())
	}
	if ctl.CurrentStore() != nil || hub.active.Load() {
		t.Fatal("store still running or subscribed after disable")
	}
	if manager.LogStore().Enabled {
		t.Fatal("config not persisted disabled")
	}

	w = doRequest(t, router, http.MethodGet, "/api/v1/history/status", "")
	if !strings.Contains(w.Body.String(), `"enabled":false`) {
		t.Fatalf("status after disable = %s, want enabled:false", w.Body.String())
	}
	if w := doRequest(t, router, http.MethodGet, "/api/v1/history/logs?container=web", ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("history query after disable = %d, want 503", w.Code)
	}
	if w := doRequest(t, router, http.MethodDelete, "/api/v1/history/containers/web?host=local", ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("history purge after disable = %d, want 503", w.Code)
	}

	// Re-enable reopens the same database and reattaches the subscription.
	w = doRequest(t, router, http.MethodPut, "/api/v1/settings/log-storage", `{"enabled":true}`)
	if w.Code != http.StatusOK {
		t.Fatalf("re-enable = %d, want 200: %s", w.Code, w.Body.String())
	}
	if ctl.CurrentStore() == nil || !hub.active.Load() {
		t.Fatal("store not restarted on re-enable")
	}
	w = doRequest(t, router, http.MethodGet, "/api/v1/history/status", "")
	if !strings.Contains(w.Body.String(), `"enabled":true`) {
		t.Fatalf("status after re-enable = %s, want enabled:true", w.Body.String())
	}
}

func TestUpdateLogStorageRejectsEnvPinnedEnabled(t *testing.T) {
	router, _, ctl, _ := newToggleRouter(t, filepath.Join(t.TempDir(), "config.json"))
	t.Setenv("LOG_STORE_ENABLED", "false")

	w := doRequest(t, router, http.MethodPut, "/api/v1/settings/log-storage", `{"enabled":true}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("PUT = %d, want 409 for an env-pinned enabled flag: %s", w.Code, w.Body.String())
	}
	if ctl.CurrentStore() != nil {
		t.Fatal("env-pinned toggle must not start a store")
	}
}

// A store that cannot be opened is a degraded service: the API must report it
// instead of answering 200, the config stays unchanged, and the same request
// succeeds once the fault clears.
func TestUpdateLogStorageEnableFailureIs503AndRetryable(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	router, _, ctl, _ := newToggleRouter(t, configPath)

	// A corrupt database beside the config makes Open fail at schema init,
	// without affecting the config file itself.
	corrupt := filepath.Join(dir, "logs.db")
	if err := os.WriteFile(corrupt, []byte("this is not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}

	w := doRequest(t, router, http.MethodPut, "/api/v1/settings/log-storage", `{"enabled":true}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("enable against a corrupt database = %d, want 503: %s", w.Code, w.Body.String())
	}
	if ctl.CurrentStore() != nil {
		t.Fatal("failed enable must not publish a store")
	}

	// Fault clears (the corrupt file is removed), the same operation retries.
	if err := os.Remove(corrupt); err != nil {
		t.Fatal(err)
	}
	w = doRequest(t, router, http.MethodPut, "/api/v1/settings/log-storage", `{"enabled":true}`)
	if w.Code != http.StatusOK {
		t.Fatalf("retry after the fault cleared = %d, want 200: %s", w.Code, w.Body.String())
	}
	if ctl.CurrentStore() == nil {
		t.Fatal("retry did not start the store")
	}
}

func TestUpdateLogStorageToggleDuringShutdownUnavailable(t *testing.T) {
	router, _, ctl, _ := newToggleRouter(t, filepath.Join(t.TempDir(), "config.json"))
	ctl.Shutdown()

	w := doRequest(t, router, http.MethodPut, "/api/v1/settings/log-storage", `{"enabled":true}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("enable during shutdown = %d, want 503: %s", w.Code, w.Body.String())
	}
}
