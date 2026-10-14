// Package execsession turns the read-only switch into an observable execution
// boundary: it admits or rejects new terminal/exec sessions, tracks every
// running Docker exec, and terminates them when the instance enters read-only
// mode or shuts down.
//
// The package is transport-agnostic. Each session carries a terminate callback
// supplied by its owner (the HTTP handlers); the callback closes the WebSocket
// with a distinguishable close frame, tears down the hijacked Docker
// connection, or cancels a one-shot command. Owners MUST call Release when the
// session ends for any reason so the registry entry and the underlying
// resources are freed.
package execsession

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNotReadOnly is returned by RetryCleanup when the instance is writable.
var ErrNotReadOnly = errors.New("instance is not in read-only mode")

// Kind distinguishes interactive terminals from one-shot commands.
type Kind string

const (
	KindTerminal Kind = "terminal"
	KindCommand  Kind = "command"
)

// Reason explains why a session is terminated, so callers can tell a
// read-only revocation apart from a shutdown or an ordinary client disconnect.
type Reason string

const (
	ReasonReadOnly Reason = "read_only"
	ReasonShutdown Reason = "server_shutdown"
)

// WebSocket close codes in the 4000-4999 application-private range (RFC 6455).
// A network drop produces no close frame at all (browser code 1006), so these
// codes let clients distinguish a deliberate boundary close from a broken
// connection.
const (
	CloseCodeReadOnly = 4003
	CloseCodeShutdown = 4000
)

const (
	CloseTextReadOnly = "instance entered read-only mode"
	CloseTextShutdown = "server is shutting down"
)

// CloseFrame maps a termination reason to the WebSocket close code and text.
func CloseFrame(reason Reason) (code int, text string) {
	switch reason {
	case ReasonShutdown:
		return CloseCodeShutdown, CloseTextShutdown
	default:
		return CloseCodeReadOnly, CloseTextReadOnly
	}
}

// PendingItem describes a session that termination was requested for but which
// had not released its resources when the bounded cleanup wait elapsed. These
// items stay tracked, keep blocking nothing new (admission is sealed), and can
// be retried.
type PendingItem struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Host      string    `json:"host"`
	Container string    `json:"container"`
	ExecID    string    `json:"execId,omitempty"`
	StartedAt time.Time `json:"startedAt"`
	Reason    string    `json:"reason"`
}

// Report is the observable result of a cleanup pass.
type Report struct {
	// Terminated counts sessions that released within the wait budget.
	Terminated int `json:"terminated"`
	// Pending lists sessions still holding resources after the wait.
	Pending []PendingItem `json:"pending"`
	// TimedOut is true when at least one session outlived the wait budget.
	TimedOut bool `json:"timedOut"`
}

// Session is one admitted terminal or one-shot command.
type Session struct {
	ID        string
	Kind      Kind
	Host      string
	Container string
	StartedAt time.Time

	execID atomic.Pointer[string]

	supervisor *Supervisor
	done       chan struct{}

	mu         sync.Mutex
	terminate  func(Reason)
	terminated bool
	reason     Reason

	releaseOnce sync.Once
}

// SetExecID records the Docker exec id once it has been created, so cleanup
// reports can name the exact daemon-side exec.
func (s *Session) SetExecID(id string) {
	s.execID.Store(&id)
}

// ExecID returns the recorded Docker exec id, or "" before creation.
func (s *Session) ExecID() string {
	if p := s.execID.Load(); p != nil {
		return *p
	}
	return ""
}

// Bind installs (or replaces) the terminate callback. When termination has
// already been requested before the callback existed, the callback is invoked
// immediately with that reason, so a resource created during the switch still
// gets torn down.
func (s *Session) Bind(fn func(Reason)) {
	s.mu.Lock()
	if !s.terminated {
		s.terminate = fn
		s.mu.Unlock()
		return
	}
	reason := s.reason
	// Keep the newest callback installed; previously installed callbacks have
	// already been invoked by Terminate.
	s.terminate = fn
	s.mu.Unlock()
	fn(reason)
}

// Terminate requests termination exactly once, regardless of how many times
// or from whom it is called (read-only switch, shutdown, later retry).
func (s *Session) Terminate(reason Reason) {
	s.mu.Lock()
	if s.terminated {
		s.mu.Unlock()
		return
	}
	s.terminated = true
	s.reason = reason
	fn := s.terminate
	s.mu.Unlock()
	if fn != nil {
		fn(reason)
	}
}

// TerminationCause reports why the session was terminated, if it was. Callers
// use it to answer clients with a revocation result instead of a generic error.
func (s *Session) TerminationCause() (Reason, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reason, s.terminated
}

// Done closes when Release has run.
func (s *Session) Done() <-chan struct{} { return s.done }

func (s *Session) released() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Release unregisters the session. It is safe to call repeatedly and from any
// goroutine; owners defer it so client disconnect, handler errors, hot-swaps
// and shutdown all free the registry entry.
func (s *Session) Release() {
	s.releaseOnce.Do(func() {
		s.supervisor.remove(s.ID)
		close(s.done)
	})
}

// Supervisor is the admission gate and the live registry of exec sessions.
type Supervisor struct {
	// transMu serializes enable/disable transitions, so a concurrent toggle
	// cannot interleave seal/persist/cleanup with the opposite direction.
	transMu sync.Mutex

	mu       sync.Mutex
	sealed   bool
	sessions map[string]*Session
	nextID   atomic.Uint64
}

// New creates a supervisor. initiallyReadOnly must reflect the effective
// startup config: an instance booted under READONLY_MODE starts sealed, so new
// sessions are refused without needing a transition.
func New(startupReadOnly bool) *Supervisor {
	return &Supervisor{
		sealed:   startupReadOnly,
		sessions: make(map[string]*Session),
	}
}

// IsSealed reports whether new exec sessions must be refused. Read by the
// read-only middleware on every request.
func (s *Supervisor) IsSealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

// Admit registers a new session. It returns nil while sealed (read-only entry
// in progress or active), which the owner turns into a 403 before hijacking
// the connection or creating any Docker exec.
func (s *Supervisor) Admit(kind Kind, host, container string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return nil
	}
	idNum := s.nextID.Add(1)
	id := ""
	switch kind {
	case KindTerminal:
		id = "terminal-"
	default:
		id = "command-"
	}
	id += uintToString(idNum)
	sess := &Session{
		ID:         id,
		Kind:       kind,
		Host:       host,
		Container:  container,
		StartedAt:  time.Now(),
		supervisor: s,
		done:       make(chan struct{}),
	}
	s.sessions[id] = sess
	return sess
}

// Transition drives one read-only toggle, serialized against other toggles.
//
// Entering seals admission first, then calls persist: a persistence failure
// rolls the seal back and never touches existing sessions. After a successful
// persist every live session is terminated, and the call waits up to timeout
// for their release; leftovers are reported, stay tracked and remain retryable
// while admission stays sealed.
//
// Exiting persists first and only unseals on success, so a failed write keeps
// the instance read-only. Sessions terminated by the prior entry stay dead;
// unsealing admits only brand-new sessions.
//
// Same-direction calls are idempotent: persist still runs (preserving the
// legacy file-rewrite behavior), and re-entering re-attempts leftover cleanup.
func (s *Supervisor) Transition(enter bool, persist func() error, timeout time.Duration) (Report, error) {
	s.transMu.Lock()
	defer s.transMu.Unlock()

	sealed := s.IsSealed()
	switch {
	case enter && !sealed:
		s.setSealed(true)
		if err := persist(); err != nil {
			// Config did not change: reopen admission and leave sessions alone.
			s.setSealed(false)
			return Report{}, err
		}
		return s.cleanup(ReasonReadOnly, timeout), nil
	case enter:
		// Already read-only: persist keeps the legacy rewrite semantics and
		// cleanup retries whatever a previous pass could not finish.
		if err := persist(); err != nil {
			return Report{}, err
		}
		return s.cleanup(ReasonReadOnly, timeout), nil
	case !enter && sealed:
		if err := persist(); err != nil {
			// Stay sealed on a write failure rather than masking the risk.
			return Report{}, err
		}
		s.setSealed(false)
		return Report{}, nil
	default:
		// Writable -> writable: nothing to seal or clean up.
		return Report{}, persist()
	}
}

// RetryCleanup re-attempts termination of leftover sessions while read-only.
// It fails when the instance is no longer sealed, so callers cannot use it to
// interfere with sessions admitted after switching back to writable.
func (s *Supervisor) RetryCleanup(timeout time.Duration) (Report, error) {
	s.transMu.Lock()
	defer s.transMu.Unlock()
	if !s.IsSealed() {
		return Report{}, ErrNotReadOnly
	}
	return s.cleanup(ReasonReadOnly, timeout), nil
}

// Shutdown terminates every live session with the shutdown reason and waits
// bounded time for the underlying connections and commands to release.
func (s *Supervisor) Shutdown(timeout time.Duration) Report {
	s.transMu.Lock()
	defer s.transMu.Unlock()
	return s.cleanup(ReasonShutdown, timeout)
}

// Pending returns the sessions still holding resources. The settings API
// exposes these while read-only so operators can see partial cleanup failures
// and retry them.
func (s *Supervisor) Pending() []PendingItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]PendingItem, 0, len(s.sessions))
	for _, sess := range s.sessions {
		items = append(items, pendingItem(sess))
	}
	return items
}

// cleanup fans termination out to every tracked session, then bounds the wait
// for their release by the overall timeout. Callers must hold transMu.
func (s *Supervisor) cleanup(reason Reason, timeout time.Duration) Report {
	s.mu.Lock()
	snap := make([]*Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		snap = append(snap, sess)
	}
	s.mu.Unlock()

	for _, sess := range snap {
		sess.Terminate(reason)
	}
	if len(snap) > 0 {
		slog.Info("read-only boundary: terminating exec sessions",
			"reason", string(reason), "sessions", len(snap))
	}

	deadline := time.Now().Add(timeout)
	pending := make([]PendingItem, 0)
	for _, sess := range snap {
		wait := time.Until(deadline)
		if wait < 0 {
			wait = 0
		}
		select {
		case <-sess.Done():
		case <-time.After(wait):
		}
		if !sess.released() {
			pending = append(pending, pendingItem(sess))
		}
	}

	report := Report{
		Terminated: len(snap) - len(pending),
		Pending:    pending,
		TimedOut:   len(pending) > 0,
	}
	if report.TimedOut {
		slog.Warn("read-only boundary: some exec sessions did not release in time",
			"reason", string(reason), "terminated", report.Terminated, "pending", len(pending))
	}
	return report
}

func pendingItem(sess *Session) PendingItem {
	reason, _ := sess.TerminationCause()
	return PendingItem{
		ID:        sess.ID,
		Kind:      string(sess.Kind),
		Host:      sess.Host,
		Container: sess.Container,
		ExecID:    sess.ExecID(),
		StartedAt: sess.StartedAt,
		Reason:    string(reason),
	}
}

func (s *Supervisor) setSealed(sealed bool) {
	s.mu.Lock()
	s.sealed = sealed
	s.mu.Unlock()
}

func (s *Supervisor) remove(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

func uintToString(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
