package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/AmoabaKelvin/logdeck/internal/config"
	"github.com/AmoabaKelvin/logdeck/internal/execsession"
	"github.com/AmoabaKelvin/logdeck/internal/services"
)

// testExecSupervisor gives the shared router-test builders a writable
// exec-session registry; tests for the read-only boundary build their own.
func testExecSupervisor() *execsession.Supervisor {
	return execsession.New(false)
}

func newBoundaryRouter(t *testing.T, readonlyEnv string) (http.Handler, *config.Manager, *execsession.Supervisor) {
	t.Helper()
	for _, key := range []string{
		"JWT_SECRET", "ADMIN_USERNAME", "ADMIN_PASSWORD", "ADMIN_PASSWORD_SALT",
		"DOCKER_HOSTS", "COOLIFY_CONFIGS", "READONLY_MODE", "CORS_ALLOWED_ORIGINS",
	} {
		t.Setenv(key, "")
	}
	if readonlyEnv != "" {
		t.Setenv("READONLY_MODE", readonlyEnv)
	}
	t.Setenv("CONFIG_PATH", filepath.Join(t.TempDir(), "config.json"))

	manager := config.NewManager()
	registry := services.NewRegistry(nil, nil, nil, manager.Config())
	// Mirror main.go: merged config changes propagate into the registry.
	manager.OnChange(registry.UpdateConfig)
	supervisor := execsession.New(manager.Config().ReadOnly)
	return NewRouter(registry, manager, nil, supervisor, nil, "test"), manager, supervisor
}

func putReadOnly(t *testing.T, router http.Handler, value string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut,
		"/api/v1/settings/read-only", strings.NewReader(`{"value":`+value+`}`)))
	return w
}

func decodeCleanup(t *testing.T, w *httptest.ResponseRecorder) struct {
	Message  string `json:"message"`
	ReadOnly bool   `json:"readOnly"`
	Cleanup  struct {
		Terminated int  `json:"terminated"`
		TimedOut   bool `json:"timedOut"`
		Pending    []map[string]any
	} `json:"cleanup"`
} {
	t.Helper()
	var body struct {
		Message  string `json:"message"`
		ReadOnly bool   `json:"readOnly"`
		Cleanup  struct {
			Terminated int  `json:"terminated"`
			TimedOut   bool `json:"timedOut"`
			Pending    []map[string]any
		} `json:"cleanup"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response %q: %v", w.Body.String(), err)
	}
	return body
}

// TestUpdateReadOnlyBoundaryTerminatesActiveSession covers the full boundary
// through the settings API: admission is sealed, the live session is
// terminated and releases, and the response reports it.
func TestUpdateReadOnlyBoundaryTerminatesActiveSession(t *testing.T) {
	router, manager, supervisor := newBoundaryRouter(t, "")

	sess := supervisor.Admit(execsession.KindTerminal, "prod", "c1")
	if sess == nil {
		t.Fatal("expected writable admission")
	}
	var terminated atomic.Bool
	sess.Bind(func(execsession.Reason) {
		terminated.Store(true)
		sess.Release()
	})

	w := putReadOnly(t, router, "true")
	if w.Code != http.StatusOK {
		t.Fatalf("PUT = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeCleanup(t, w)
	if !body.ReadOnly || body.Cleanup.Terminated != 1 || body.Cleanup.TimedOut {
		t.Fatalf("unexpected response: %+v", body)
	}
	if !terminated.Load() {
		t.Fatal("the live session was not terminated")
	}
	if !supervisor.IsSealed() {
		t.Fatal("admission not sealed after enabling read-only")
	}
	if supervisor.Admit(execsession.KindCommand, "prod", "c1") != nil {
		t.Fatal("new exec session admitted after the boundary closed")
	}
	ro := manager.FileConfigSnapshot().ReadOnly
	if ro == nil || !*ro {
		t.Fatal("read-only was not persisted to the config file")
	}
}

// TestUpdateReadOnlyNoActiveExecsKeepsLegacyBehavior proves the no-exec path
// behaves as before apart from the additive cleanup report, and switching
// back reopens admission.
func TestUpdateReadOnlyNoActiveExecsKeepsLegacyBehavior(t *testing.T) {
	router, _, supervisor := newBoundaryRouter(t, "")

	w := putReadOnly(t, router, "true")
	if w.Code != http.StatusOK {
		t.Fatalf("PUT = %d: %s", w.Code, w.Body.String())
	}
	body := decodeCleanup(t, w)
	if body.Message != "Read-only mode updated" {
		t.Fatalf("message = %q", body.Message)
	}
	if body.Cleanup.Terminated != 0 || body.Cleanup.TimedOut || len(body.Cleanup.Pending) != 0 {
		t.Fatalf("cleanup should be empty with no execs, got %+v", body.Cleanup)
	}

	if w := putReadOnly(t, router, "false"); w.Code != http.StatusOK {
		t.Fatalf("disable = %d: %s", w.Code, w.Body.String())
	}
	if supervisor.IsSealed() {
		t.Fatal("admission still sealed after switching back to writable")
	}
	if supervisor.Admit(execsession.KindTerminal, "prod", "c1") == nil {
		t.Fatal("new terminal not admitted after writable switch")
	}
}

// TestUpdateReadOnlyEnvLockedRejected verifies an env-pinned read-only setting
// keeps its 409 behavior on both directions and never changes the seal: an
// env-locked instance boots sealed and stays sealed.
func TestUpdateReadOnlyEnvLockedRejected(t *testing.T) {
	router, _, supervisor := newBoundaryRouter(t, "true")

	if !supervisor.IsSealed() {
		t.Fatal("env-locked read-only must seal at startup")
	}
	if supervisor.Admit(execsession.KindTerminal, "prod", "c1") != nil {
		t.Fatal("env-locked instance admitted an exec session")
	}

	for _, value := range []string{"true", "false"} {
		w := putReadOnly(t, router, value)
		if w.Code != http.StatusConflict {
			t.Fatalf("PUT value=%s = %d, want 409: %s", value, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "environment variable") {
			t.Fatalf("missing env-lock message: %s", w.Body.String())
		}
		if !supervisor.IsSealed() {
			t.Fatalf("seal changed after rejected env-locked update (value=%s)", value)
		}
	}
}

// TestRetryReadOnlyCleanup covers the retry endpoint: available while
// read-only, 409 once writable.
func TestRetryReadOnlyCleanup(t *testing.T) {
	router, _, supervisor := newBoundaryRouter(t, "")

	if w := putReadOnly(t, router, "true"); w.Code != http.StatusOK {
		t.Fatalf("enable = %d: %s", w.Code, w.Body.String())
	}

	post := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPost,
			"/api/v1/settings/read-only/cleanup", nil))
		return w
	}
	if w := post(); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"readOnly":true`) {
		t.Fatalf("retry while read-only = %d: %s", w.Code, w.Body.String())
	}

	if w := putReadOnly(t, router, "false"); w.Code != http.StatusOK {
		t.Fatalf("disable = %d: %s", w.Code, w.Body.String())
	}
	if w := post(); w.Code != http.StatusConflict {
		t.Fatalf("retry while writable = %d, want 409: %s", w.Code, w.Body.String())
	}
	_ = supervisor
}

// TestGetSettingsReportsEmptyCleanupPending ensures the readOnly block keeps
// its old fields and adds the additive cleanupPending list.
func TestGetSettingsReportsEmptyCleanupPending(t *testing.T) {
	router, _, _ := newBoundaryRouter(t, "")
	if w := putReadOnly(t, router, "true"); w.Code != http.StatusOK {
		t.Fatalf("enable = %d", w.Code)
	}

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET settings = %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		ReadOnly struct {
			Source         string           `json:"source"`
			Value          bool             `json:"value"`
			CleanupPending []map[string]any `json:"cleanupPending"`
		} `json:"readOnly"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.ReadOnly.Value || body.ReadOnly.Source != "file" {
		t.Fatalf("readOnly block = %+v", body.ReadOnly)
	}
	if body.ReadOnly.CleanupPending == nil {
		t.Fatal("cleanupPending should be an empty list, not null")
	}
}
