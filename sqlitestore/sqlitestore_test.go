package sqlitestore_test

import (
	"context"
	"database/sql"
	"errors"
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

func openDB(t *testing.T) (*sql.DB, *sqlitestore.Store) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "durable.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := sqlitestore.New(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return db, store
}

func open(t *testing.T) durable.Store {
	_, store := openDB(t)
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

func TestCreateRunTx(t *testing.T) {
	db, store := openDB(t)
	e := durable.New(store, durable.PollInterval(time.Hour), durable.Logger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	greet := durable.Register(e, "greet", func(ctx durable.Context, name string) (string, error) {
		return durable.Step(ctx, "greet", func(context.Context) (string, error) { return "hi " + name, nil })
	})
	other := durable.Register(e, "other", func(durable.Context, string) (string, error) { return "", nil })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	withOrder := func(commit bool, wf *durable.Workflow[string, string], id, name string) error {
		run, err := wf.NewRun(name, durable.WithID(id))
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if err := store.CreateRunTx(context.Background(), tx, run); err != nil {
			return err
		}
		if !commit {
			return nil
		}
		return tx.Commit()
	}

	if err := withOrder(false, greet, "rolled-back", "nobody"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRun(context.Background(), "rolled-back"); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("a rolled back run exists: %v", err)
	}

	if err := withOrder(true, greet, "order-1", "ada"); err != nil {
		t.Fatal(err)
	}
	e.Wake()
	waitCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	got, err := greet.Handle("order-1").Result(waitCtx)
	if err != nil || got != "hi ada" {
		t.Fatalf("result = %q, %v", got, err)
	}

	if err := withOrder(true, greet, "order-1", "grace"); err != nil {
		t.Fatalf("same id again: %v", err)
	}
	if run, err := store.GetRun(context.Background(), "order-1"); err != nil || string(run.Input) != `"ada"` || run.Status != durable.StatusSucceeded {
		t.Fatalf("the second create changed the run: %+v %v", run, err)
	}
	if err := withOrder(true, other, "order-1", "ada"); !errors.Is(err, durable.ErrIDConflict) {
		t.Fatalf("another workflow's id: %v", err)
	}
}
