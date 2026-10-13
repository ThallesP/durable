package sqlitestore_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThallesP/durable"
	"github.com/ThallesP/durable/durabletest"
	"github.com/ThallesP/durable/sqlitestore"
	_ "modernc.org/sqlite"
)

func open(t *testing.T) durable.Store {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "durable.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := sqlitestore.New(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestStore(t *testing.T) {
	durabletest.Store(t, open)
}

func TestEngineResumesFromSQLite(t *testing.T) {
	store := open(t)
	var charged atomic.Int32
	run := func(register func(e *durable.Engine)) func() {
		e := durable.New(store, durable.PollInterval(10*time.Millisecond), durable.Logger(slog.New(slog.NewTextHandler(io.Discard, nil))))
		register(e)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			e.Run(ctx)
			close(done)
		}()
		return func() {
			cancel()
			<-done
		}
	}
	checkout := func(ctx durable.Context, total int) (string, error) {
		if err := durable.Do(ctx, "charge", func(context.Context) error {
			charged.Add(1)
			return nil
		}); err != nil {
			return "", err
		}
		return durable.Await[string](ctx, "shipped", time.Minute)
	}

	var wf *durable.Workflow[int, string]
	stop := run(func(e *durable.Engine) { wf = durable.Register(e, "checkout", checkout) })
	h, err := wf.Start(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		run, err := store.GetRun(context.Background(), h.ID())
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == durable.StatusWaiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run is %s", run.Status)
		}
	}
	stop()

	var e *durable.Engine
	defer run(func(next *durable.Engine) {
		e = next
		wf = durable.Register(next, "checkout", checkout)
	})()
	if err := e.Signal(context.Background(), h.ID(), "shipped", "box-1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := wf.Handle(h.ID()).Result(ctx)
	if err != nil || got != "box-1" {
		t.Fatalf("result = %q, %v", got, err)
	}
	if charged.Load() != 1 {
		t.Fatalf("charged %d times", charged.Load())
	}
}
