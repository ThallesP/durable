package durable_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ThallesP/durable"
	"github.com/ThallesP/durable/memstore"
)

type engine struct {
	*durable.Engine
	stop func()
}

func newEngine(t *testing.T, store durable.Store, register func(e *durable.Engine), opts ...durable.Option) *engine {
	t.Helper()
	e := durable.New(store, append([]durable.Option{
		durable.PollInterval(10 * time.Millisecond),
		durable.Lease(time.Second),
		durable.ShutdownGrace(50 * time.Millisecond),
		durable.Logger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}, opts...)...)
	register(e)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		if err := e.Run(ctx); err != nil {
			t.Error(err)
		}
		close(done)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	t.Cleanup(stop)
	return &engine{Engine: e, stop: stop}
}

func waitStatus(t *testing.T, e *engine, id string, want durable.Status) durable.RunInfo {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := e.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if info.Run.Status == want {
			return info
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s is %s (%s), want %s", id, info.Run.Status, info.Run.Error, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func result[O any](t *testing.T, h *durable.Handle[O]) (O, error) {
	t.Helper()
	return resultWithin(t, h, 5*time.Second)
}

func resultWithin[O any](t *testing.T, h *durable.Handle[O], timeout time.Duration) (O, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return h.Result(ctx)
}

type counter struct{ n atomic.Int32 }

func (c *counter) inc(context.Context) (int, error) { return int(c.n.Add(1)), nil }

type order struct{ Total int }

func TestStepsAreNotRepeatedAfterAnUpgrade(t *testing.T) {
	store := memstore.New()
	var charged, shipped, audited counter

	v1 := newEngine(t, store, func(e *durable.Engine) {
		durable.Register(e, "checkout", func(ctx durable.Context, in order) (int, error) {
			if _, err := durable.Step(ctx, "charge", charged.inc); err != nil {
				return 0, err
			}
			if _, err := durable.Await[string](ctx, "approved", 0); err != nil {
				return 0, err
			}
			return durable.Step(ctx, "ship", shipped.inc)
		})
	})
	wf := durable.Register(durable.New(store), "checkout", func(durable.Context, order) (int, error) { return 0, nil })
	h, err := wf.Start(context.Background(), order{Total: 10}, durable.WithID("order-1"))
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, v1, h.ID(), durable.StatusWaiting)
	v1.stop()

	v2 := newEngine(t, store, func(e *durable.Engine) {
		durable.Register(e, "checkout", func(ctx durable.Context, in order) (int, error) {
			if _, err := durable.Step(ctx, "audit", audited.inc); err != nil {
				return 0, err
			}
			if _, err := durable.Step(ctx, "charge", charged.inc); err != nil {
				return 0, err
			}
			if _, err := durable.Await[string](ctx, "approved", 0); err != nil {
				return 0, err
			}
			return durable.Step(ctx, "ship", shipped.inc)
		})
	})
	if err := v2.Signal(context.Background(), h.ID(), "approved", "yes"); err != nil {
		t.Fatal(err)
	}
	info := waitStatus(t, v2, h.ID(), durable.StatusSucceeded)

	if charged.n.Load() != 1 || shipped.n.Load() != 1 || audited.n.Load() != 1 {
		t.Fatalf("charged %d, shipped %d, audited %d times; want 1 each", charged.n.Load(), shipped.n.Load(), audited.n.Load())
	}
	if len(info.Steps) != 4 {
		t.Fatalf("saved %d steps, want 4", len(info.Steps))
	}
}

func TestSleepSurvivesARestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := memstore.New()
		var woke counter
		register := func(e *durable.Engine) *durable.Workflow[struct{}, int] {
			return durable.Register(e, "reminder", func(ctx durable.Context, _ struct{}) (int, error) {
				durable.Sleep(ctx, "a day", 24*time.Hour)
				return durable.Step(ctx, "remind", woke.inc)
			})
		}

		var wf *durable.Workflow[struct{}, int]
		first := newEngine(t, store, func(e *durable.Engine) { wf = register(e) }, durable.PollInterval(time.Minute))
		h, err := wf.Start(context.Background(), struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		waitStatus(t, first, h.ID(), durable.StatusWaiting)
		first.stop()
		time.Sleep(23 * time.Hour)

		second := newEngine(t, store, func(e *durable.Engine) { wf = register(e) }, durable.PollInterval(time.Minute))
		started := time.Now()
		if _, err := resultWithin(t, wf.Handle(h.ID()), 48*time.Hour); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(started); waited > 2*time.Hour {
			t.Fatalf("waited %s after the restart, want about an hour", waited)
		}
		if woke.n.Load() != 1 {
			t.Fatalf("remind ran %d times", woke.n.Load())
		}
		second.stop()
	})
}

func TestAwaitTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var wf *durable.Workflow[struct{}, string]
		newEngine(t, memstore.New(), func(e *durable.Engine) {
			wf = durable.Register(e, "approval", func(ctx durable.Context, _ struct{}) (string, error) {
				_, err := durable.Await[bool](ctx, "approved", time.Hour)
				if errors.Is(err, durable.ErrTimeout) {
					return "expired", nil
				}
				return "approved", err
			})
		}, durable.PollInterval(time.Minute))
		h, err := wf.Start(context.Background(), struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := resultWithin(t, h, 48*time.Hour)
		if err != nil || got != "expired" {
			t.Fatalf("result = %q, %v; want expired", got, err)
		}
	})
}

func TestSignalSentBeforeAwaitIsKept(t *testing.T) {
	release := make(chan struct{})
	var wf *durable.Workflow[struct{}, int]
	e := newEngine(t, memstore.New(), func(e *durable.Engine) {
		wf = durable.Register(e, "collect", func(ctx durable.Context, _ struct{}) (int, error) {
			err := durable.Do(ctx, "wait for test", func(context.Context) error {
				<-release
				return nil
			})
			if err != nil {
				return 0, err
			}
			first, err := durable.Await[int](ctx, "number", time.Minute)
			if err != nil {
				return 0, err
			}
			second, err := durable.Await[int](ctx, "number", time.Minute)
			return first*10 + second, err
		})
	})
	h, err := wf.Start(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, e, h.ID(), durable.StatusRunning)
	for _, n := range []int{4, 2} {
		if err := e.Signal(context.Background(), h.ID(), "number", n); err != nil {
			t.Fatal(err)
		}
	}
	close(release)

	got, err := result(t, h)
	if err != nil || got != 42 {
		t.Fatalf("result = %d, %v; want 42", got, err)
	}
}

func TestIncompatibleResultStallsTheRun(t *testing.T) {
	store := memstore.New()
	type planV1 struct{ NodeIDs []string }
	type planV2 struct{ Nodes []string }

	v1 := newEngine(t, store, func(e *durable.Engine) {
		durable.Register(e, "deploy", func(ctx durable.Context, _ struct{}) (int, error) {
			if _, err := durable.Step(ctx, "plan", func(context.Context) (planV1, error) { return planV1{NodeIDs: []string{"a"}}, nil }); err != nil {
				return 0, err
			}
			_, err := durable.Await[bool](ctx, "converged", 0)
			return 0, err
		})
	})
	wf := durable.Register(durable.New(store), "deploy", func(durable.Context, struct{}) (int, error) { return 0, nil })
	h, err := wf.Start(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, v1, h.ID(), durable.StatusWaiting)
	v1.stop()

	v2 := newEngine(t, store, func(e *durable.Engine) {
		durable.Register(e, "deploy", func(ctx durable.Context, _ struct{}) (int, error) {
			plan, err := durable.Step(ctx, "plan", func(context.Context) (planV2, error) { return planV2{Nodes: []string{"a"}}, nil })
			return len(plan.Nodes), err
		})
	})
	if err := v2.Signal(context.Background(), h.ID(), "converged", true); err != nil {
		t.Fatal(err)
	}
	info := waitStatus(t, v2, h.ID(), durable.StatusStalled)
	if !strings.Contains(info.Run.Error, `"plan"`) || !strings.Contains(info.Run.Error, "NodeIDs") {
		t.Fatalf("stall reason = %q", info.Run.Error)
	}

	if err := v2.Restart(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	got, err := result(t, wf.Handle(h.ID()))
	if err != nil || got != 1 {
		t.Fatalf("after restart: %d, %v", got, err)
	}
}

func TestUnknownWorkflowStallsUntilRegistered(t *testing.T) {
	store := memstore.New()
	wf := durable.Register(durable.New(store), "legacy", func(durable.Context, struct{}) (string, error) { return "", nil })
	h, err := wf.Start(context.Background(), struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	without := newEngine(t, store, func(*durable.Engine) {})
	info := waitStatus(t, without, h.ID(), durable.StatusStalled)
	if !strings.Contains(info.Run.Error, "not registered") {
		t.Fatalf("stall reason = %q", info.Run.Error)
	}
	without.stop()

	newEngine(t, store, func(e *durable.Engine) {
		durable.Register(e, "legacy", func(durable.Context, struct{}) (string, error) { return "done", nil })
	})
	got, err := result(t, wf.Handle(h.ID()))
	if err != nil || got != "done" {
		t.Fatalf("result = %q, %v", got, err)
	}
}

type deployInput struct{ Env string }

func TestConcurrencyPerPartition(t *testing.T) {
	var running, peak atomic.Int32
	var wf *durable.Workflow[deployInput, string]
	e := newEngine(t, memstore.New(), func(e *durable.Engine) {
		wf = durable.Register(e, "deploy", func(ctx durable.Context, in deployInput) (string, error) {
			err := durable.Do(ctx, "enter", func(context.Context) error {
				n := running.Add(1)
				if n > peak.Load() {
					peak.Store(n)
				}
				return nil
			})
			if err != nil {
				return "", err
			}
			if _, err := durable.Await[bool](ctx, "converged", 0); err != nil {
				return "", err
			}
			return in.Env, durable.Do(ctx, "leave", func(context.Context) error {
				running.Add(-1)
				return nil
			})
		}, durable.Concurrency(1), durable.PartitionBy(func(in deployInput) string { return in.Env }))
	})

	first, _ := wf.Start(context.Background(), deployInput{Env: "prod"})
	second, _ := wf.Start(context.Background(), deployInput{Env: "prod"})
	other, _ := wf.Start(context.Background(), deployInput{Env: "staging"})
	waitStatus(t, e, first.ID(), durable.StatusWaiting)
	waitStatus(t, e, other.ID(), durable.StatusWaiting)
	time.Sleep(50 * time.Millisecond)
	if info, _ := e.Get(context.Background(), second.ID()); info.Run.Status != durable.StatusPending {
		t.Fatalf("second prod deploy is %s, want pending", info.Run.Status)
	}

	for _, h := range []*durable.Handle[string]{first, other, second} {
		waitStatus(t, e, h.ID(), durable.StatusWaiting)
		if err := e.Signal(context.Background(), h.ID(), "converged", true); err != nil {
			t.Fatal(err)
		}
		if _, err := result(t, h); err != nil {
			t.Fatal(err)
		}
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency = %d, want 2 (one per environment)", peak.Load())
	}
}

func TestCancelStopsAWaitingRun(t *testing.T) {
	var wf *durable.Workflow[struct{}, string]
	e := newEngine(t, memstore.New(), func(e *durable.Engine) {
		wf = durable.Register(e, "wait", func(ctx durable.Context, _ struct{}) (string, error) {
			_, err := durable.Await[bool](ctx, "never", 0)
			return "", err
		})
	})
	h, _ := wf.Start(context.Background(), struct{}{})
	waitStatus(t, e, h.ID(), durable.StatusWaiting)
	if err := e.Cancel(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := result(t, h); !errors.Is(err, durable.ErrCancelled) {
		t.Fatalf("err = %v, want ErrCancelled", err)
	}
}

func TestCancelStopsARunningStep(t *testing.T) {
	var wf *durable.Workflow[struct{}, string]
	e := newEngine(t, memstore.New(), func(e *durable.Engine) {
		wf = durable.Register(e, "slow", func(ctx durable.Context, _ struct{}) (string, error) {
			return "", durable.Do(ctx, "block", func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			})
		})
	})
	h, _ := wf.Start(context.Background(), struct{}{})
	waitStatus(t, e, h.ID(), durable.StatusRunning)
	if err := e.Cancel(context.Background(), h.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := result(t, h); !errors.Is(err, durable.ErrCancelled) {
		t.Fatalf("err = %v, want ErrCancelled", err)
	}
}

func TestRetryAndPermanent(t *testing.T) {
	var flaky, broken counter
	var wf *durable.Workflow[struct{}, int]
	newEngine(t, memstore.New(), func(e *durable.Engine) {
		wf = durable.Register(e, "retry", func(ctx durable.Context, _ struct{}) (int, error) {
			n, err := durable.Step(ctx, "flaky", func(ctx context.Context) (int, error) {
				n, _ := flaky.inc(ctx)
				if n < 3 {
					return 0, errors.New("try again")
				}
				return n, nil
			}, durable.Retry(5, time.Millisecond))
			if err != nil {
				return 0, err
			}
			err = durable.Do(ctx, "broken", func(ctx context.Context) error {
				broken.inc(ctx)
				return durable.Permanent(errors.New("bad input"))
			}, durable.Retry(5, time.Millisecond))
			var stepErr *durable.StepError
			if !errors.As(err, &stepErr) || stepErr.Step != "broken" {
				return 0, errors.New("want a StepError for broken")
			}
			return n, nil
		})
	})
	h, _ := wf.Start(context.Background(), struct{}{})
	got, err := result(t, h)
	if err != nil || got != 3 {
		t.Fatalf("result = %d, %v", got, err)
	}
	if broken.n.Load() != 1 {
		t.Fatalf("broken ran %d times, want 1", broken.n.Load())
	}
}

func TestStartIsIdempotent(t *testing.T) {
	var ran counter
	var wf *durable.Workflow[struct{}, int]
	newEngine(t, memstore.New(), func(e *durable.Engine) {
		wf = durable.Register(e, "once", func(ctx durable.Context, _ struct{}) (int, error) {
			return durable.Step(ctx, "run", ran.inc)
		})
		durable.Register(e, "other", func(durable.Context, struct{}) (int, error) { return 0, nil })
	})
	for range 3 {
		h, err := wf.Start(context.Background(), struct{}{}, durable.WithID("same"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := result(t, h); err != nil {
			t.Fatal(err)
		}
	}
	if ran.n.Load() != 1 {
		t.Fatalf("ran %d times", ran.n.Load())
	}
}

func TestShutdownMidStepResumesElsewhere(t *testing.T) {
	store := memstore.New()
	var attempts counter
	register := func(e *durable.Engine) *durable.Workflow[struct{}, int] {
		return durable.Register(e, "job", func(ctx durable.Context, _ struct{}) (int, error) {
			return durable.Step(ctx, "work", func(ctx context.Context) (int, error) {
				n, _ := attempts.inc(ctx)
				if n == 1 {
					<-ctx.Done()
					return 0, ctx.Err()
				}
				return n, nil
			})
		})
	}
	var wf *durable.Workflow[struct{}, int]
	first := newEngine(t, store, func(e *durable.Engine) { wf = register(e) })
	h, _ := wf.Start(context.Background(), struct{}{})
	waitStatus(t, first, h.ID(), durable.StatusRunning)
	first.stop()
	if info, _ := first.Get(context.Background(), h.ID()); len(info.Steps) != 0 {
		t.Fatalf("the interrupted step was saved: %+v", info.Steps)
	}

	newEngine(t, store, func(e *durable.Engine) { wf = register(e) })
	got, err := result(t, wf.Handle(h.ID()))
	if err != nil || got != 2 {
		t.Fatalf("result = %d, %v", got, err)
	}
}

func TestNestedStepFailsTheStep(t *testing.T) {
	var wf *durable.Workflow[struct{}, string]
	newEngine(t, memstore.New(), func(e *durable.Engine) {
		wf = durable.Register(e, "nested", func(ctx durable.Context, _ struct{}) (string, error) {
			err := durable.Do(ctx, "outer", func(context.Context) error {
				return durable.Do(ctx, "inner", func(context.Context) error { return nil })
			})
			return "", err
		})
	})
	h, _ := wf.Start(context.Background(), struct{}{})
	_, err := result(t, h)
	if err == nil || !strings.Contains(err.Error(), "inside a step") {
		t.Fatalf("err = %v", err)
	}
}
