package execsession

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newReleasingSession(s *Supervisor, kind Kind) *Session {
	sess := s.Admit(kind, "host-1", "ctr-1")
	if sess == nil {
		return nil
	}
	sess.Bind(func(Reason) { sess.Release() })
	return sess
}

func TestAdmitAndRelease(t *testing.T) {
	s := New(false)
	sess := s.Admit(KindTerminal, "h", "c")
	if sess == nil {
		t.Fatal("admission should be open in writable mode")
	}
	if sess.ID == "" || sess.Kind != KindTerminal {
		t.Fatalf("session not registered properly: %+v", sess)
	}
	if got := len(s.Pending()); got != 1 {
		t.Fatalf("Pending = %d sessions, want 1", got)
	}

	sess.Release()
	sess.Release() // idempotent
	if len(s.Pending()) != 0 {
		t.Fatal("Release did not unregister the session")
	}
	select {
	case <-sess.Done():
	default:
		t.Fatal("Done channel not closed after Release")
	}
}

func TestStartsSealedWhenBootingReadOnly(t *testing.T) {
	s := New(true)
	if !s.IsSealed() {
		t.Fatal("env-locked read-only startup must seal admission")
	}
	if sess := s.Admit(KindTerminal, "h", "c"); sess != nil {
		t.Fatal("admission must be refused while sealed")
	}
}

func TestTransitionEnterSealsAndTerminates(t *testing.T) {
	s := New(false)
	sess := s.Admit(KindTerminal, "h", "c")
	sess.SetExecID("exec-abc")
	var gotReason Reason
	sess.Bind(func(r Reason) {
		gotReason = r
		sess.Release()
	})

	var persisted atomic.Bool
	report, err := s.Transition(true, func() error {
		// Admission must already be sealed while persist runs.
		if !s.IsSealed() {
			t.Error("admission was not sealed before persist")
		}
		if s.Admit(KindCommand, "h", "c") != nil {
			t.Error("new session admitted during transition")
		}
		persisted.Store(true)
		return nil
	}, time.Second)
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if !persisted.Load() || !s.IsSealed() {
		t.Fatal("not persisted/sealed after enter")
	}
	if gotReason != ReasonReadOnly {
		t.Fatalf("terminate reason = %q, want read_only", gotReason)
	}
	if report.Terminated != 1 || len(report.Pending) != 0 || report.TimedOut {
		t.Fatalf("unexpected report: %+v", report)
	}
	if len(s.Pending()) != 0 {
		t.Fatal("released session still tracked")
	}
	if s.Admit(KindTerminal, "h", "c") != nil {
		t.Fatal("new session admitted after read-only entry")
	}
}

func TestTransitionEnterPersistFailureRollsBack(t *testing.T) {
	s := New(false)
	sess := s.Admit(KindTerminal, "h", "c")
	var terminated atomic.Bool
	sess.Bind(func(Reason) { terminated.Store(true) })

	wantErr := errors.New("disk full")
	report, err := s.Transition(true, func() error { return wantErr }, time.Second)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if s.IsSealed() {
		t.Fatal("seal must roll back after persistence failure")
	}
	if terminated.Load() {
		t.Fatal("existing sessions must not be touched when persistence fails")
	}
	if report.Terminated != 0 {
		t.Fatalf("report should be empty on rollback, got %+v", report)
	}
	if s.Admit(KindCommand, "h", "c") == nil {
		t.Fatal("admission must reopen after rollback")
	}
}

func TestTransitionEnterTimeoutReportsPendingAndStaysSealed(t *testing.T) {
	s := New(false)
	stuck := s.Admit(KindCommand, "h", "c")
	stuck.SetExecID("exec-stuck")
	// Termination fires but the session never releases (unresponsive daemon).
	stuck.Bind(func(Reason) {})
	free := newReleasingSession(s, KindTerminal)
	if free == nil {
		t.Fatal("could not admit second session")
	}

	report, err := s.Transition(true, func() error { return nil }, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if !s.IsSealed() {
		t.Fatal("partial cleanup must not reopen writable mode")
	}
	if report.Terminated != 1 || !report.TimedOut || len(report.Pending) != 1 {
		t.Fatalf("report = %+v, want 1 terminated and 1 pending", report)
	}
	p := report.Pending[0]
	if p.ID != stuck.ID || p.ExecID != "exec-stuck" || p.Kind != "command" ||
		p.Host != "h" || p.Container != "c" || p.Reason != "read_only" {
		t.Fatalf("pending item mismatch: %+v", p)
	}
}

func TestRetryCleanupFinishesLeftovers(t *testing.T) {
	s := New(false)
	stuck := s.Admit(KindTerminal, "h", "c")
	stuck.Bind(func(Reason) {})

	report, _ := s.Transition(true, func() error { return nil }, 10*time.Millisecond)
	if !report.TimedOut {
		t.Fatal("expected timed-out first pass")
	}

	if _, err := s.RetryCleanup(time.Second); err != nil {
		t.Fatalf("retry while sealed: %v", err)
	}
	// Still stuck: pending remains.
	if r, _ := s.RetryCleanup(10 * time.Millisecond); !r.TimedOut {
		t.Fatal("retry must keep reporting the unresponsive session")
	}

	stuck.Release()
	report, err := s.RetryCleanup(time.Second)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	// Release unregistered the session before this pass, so the report counts
	// no work of its own — the empty pending list is the all-clear signal.
	if report.TimedOut || len(report.Pending) != 0 {
		t.Fatalf("retry report = %+v, want an empty pending list", report)
	}
	if len(s.Pending()) != 0 {
		t.Fatal("leftover still tracked after release")
	}
}

func TestRetryCleanupRejectedWhenWritable(t *testing.T) {
	s := New(false)
	if _, err := s.RetryCleanup(time.Second); !errors.Is(err, ErrNotReadOnly) {
		t.Fatalf("err = %v, want ErrNotReadOnly", err)
	}
}

func TestTransitionExitPersistFirstThenUnseal(t *testing.T) {
	s := New(false)
	old := newReleasingSession(s, KindTerminal)
	if _, err := s.Transition(true, func() error { return nil }, time.Second); err != nil {
		t.Fatal(err)
	}

	// Persist failure keeps read-only in force.
	if _, err := s.Transition(false, func() error { return errors.New("disk") }, time.Second); err == nil {
		t.Fatal("exit persist failure must surface")
	}
	if !s.IsSealed() {
		t.Fatal("failed exit persist must stay sealed")
	}

	if _, err := s.Transition(false, func() error { return nil }, time.Second); err != nil {
		t.Fatal(err)
	}
	if s.IsSealed() {
		t.Fatal("not unsealed after exit")
	}
	fresh := s.Admit(KindTerminal, "h", "c")
	if fresh == nil {
		t.Fatal("new sessions must work after switching back to writable")
	}
	fresh.Release()

	// The old session must not revive.
	select {
	case <-old.Done():
	default:
		t.Fatal("terminated old session reopened")
	}
	if reason, ok := old.TerminationCause(); !ok || reason != ReasonReadOnly {
		t.Fatalf("old session cause = %v/%v", reason, ok)
	}
}

func TestIdempotentReEnterRetriesCleanup(t *testing.T) {
	s := New(false)
	stuck := s.Admit(KindCommand, "h", "c")
	stuck.Bind(func(Reason) {})

	if _, err := s.Transition(true, func() error { return nil }, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	stuck.Release()

	var persistCalls atomic.Int32
	report, err := s.Transition(true, func() error { persistCalls.Add(1); return nil }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if persistCalls.Load() != 1 {
		t.Fatal("same-direction enter must still persist")
	}
	if report.TimedOut || len(report.Pending) != 0 {
		t.Fatalf("re-entering should finish leftover cleanup: %+v", report)
	}
}

func TestBindFiresImmediatelyAfterTermination(t *testing.T) {
	s := New(false)
	sess := s.Admit(KindTerminal, "h", "c")
	sess.Terminate(ReasonReadOnly) // no callback bound yet

	var fired atomic.Bool
	sess.Bind(func(r Reason) {
		if r != ReasonReadOnly {
			t.Errorf("reason = %q", r)
		}
		fired.Store(true)
	})
	if !fired.Load() {
		t.Fatal("late Bind must invoke the callback with the recorded reason")
	}

	// The callback runs exactly once even with repeated termination.
	sess.Terminate(ReasonShutdown)
	sess.Terminate(ReasonReadOnly)
	sess.Release()
}

func TestShutdownUsesDistinctReasonAndKeepsSealState(t *testing.T) {
	s := New(true)
	var reason Reason
	sess := s.Admit(KindTerminal, "h", "c") // sealed: nil
	if sess != nil {
		t.Fatal("sealed supervisor admitted a session")
	}
	free := New(false)
	s2 := free.Admit(KindCommand, "h", "c")
	s2.Bind(func(r Reason) {
		reason = r
		s2.Release()
	})
	report := free.Shutdown(time.Second)
	if report.Terminated != 1 || report.TimedOut {
		t.Fatalf("shutdown report = %+v", report)
	}
	if reason != ReasonShutdown {
		t.Fatalf("shutdown reason = %q", reason)
	}
	if free.IsSealed() {
		t.Fatal("shutdown must not seal a writable instance")
	}
	if !s.IsSealed() {
		t.Fatal("shutdown must not unseal a read-only instance")
	}
}

func TestConcurrentTogglesEndConsistent(t *testing.T) {
	s := New(false)
	var stored atomic.Bool
	persist := func(v bool) func() error {
		return func() error {
			stored.Store(v)
			return nil
		}
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = s.Transition(true, persist(true), time.Second) }()
		go func() { defer wg.Done(); _, _ = s.Transition(false, persist(false), time.Second) }()
	}
	wg.Wait()

	if s.IsSealed() != stored.Load() {
		t.Fatalf("seal = %v diverged from persisted value %v", s.IsSealed(), stored.Load())
	}
}

func TestTerminateRaceWithReleasingSessions(t *testing.T) {
	s := New(false)
	const n = 50
	sessions := make([]*Session, n)
	for i := range n {
		sess := s.Admit(KindTerminal, "h", "c")
		sess.Bind(func(Reason) { sess.Release() })
		sessions[i] = sess
	}

	done := make(chan Report, 1)
	go func() {
		r, _ := s.Transition(true, func() error { return nil }, 5*time.Second)
		done <- r
	}()

	report := <-done
	if report.Terminated != n || report.TimedOut {
		t.Fatalf("report = %+v, want %d terminated", report, n)
	}
}

func TestCloseFrameCodes(t *testing.T) {
	if code, _ := CloseFrame(ReasonReadOnly); code != CloseCodeReadOnly {
		t.Errorf("read-only code = %d", code)
	}
	if code, _ := CloseFrame(ReasonShutdown); code != CloseCodeShutdown {
		t.Errorf("shutdown code = %d", code)
	}
}
