package durable

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Engine struct {
	store   Store
	owner   string
	workers int
	lease   time.Duration
	poll    time.Duration
	grace   time.Duration
	log     *slog.Logger

	workflows map[string]*registration
	draining  atomic.Bool
	pokes     chan struct{}
	hard      context.Context
	hardStop  context.CancelCauseFunc
	runs      sync.WaitGroup

	mu      sync.Mutex
	started bool
	active  map[string]context.CancelCauseFunc
	changed chan struct{}
}

type Option func(*Engine)

func Workers(n int) Option                 { return func(e *Engine) { e.workers = n } }
func Lease(d time.Duration) Option         { return func(e *Engine) { e.lease = d } }
func PollInterval(d time.Duration) Option  { return func(e *Engine) { e.poll = d } }
func ShutdownGrace(d time.Duration) Option { return func(e *Engine) { e.grace = d } }
func Logger(log *slog.Logger) Option       { return func(e *Engine) { e.log = log } }
func Owner(owner string) Option            { return func(e *Engine) { e.owner = owner } }

func New(store Store, opts ...Option) *Engine {
	host, _ := os.Hostname()
	e := &Engine{
		store:     store,
		owner:     fmt.Sprintf("%s-%d-%s", host, os.Getpid(), newID()[:8]),
		workers:   16,
		lease:     30 * time.Second,
		poll:      time.Second,
		grace:     10 * time.Second,
		log:       slog.Default(),
		workflows: map[string]*registration{},
		pokes:     make(chan struct{}, 1),
		active:    map[string]context.CancelCauseFunc{},
		changed:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(e)
	}
	e.hard, e.hardStop = context.WithCancelCause(context.Background())
	return e
}

func (e *Engine) Run(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return ErrEngineState
	}
	e.started = true
	e.mu.Unlock()

	unstalled, err := e.store.Unstall(ctx, slices.Sorted(maps.Keys(e.workflows)))
	if err != nil {
		return err
	}
	if unstalled > 0 {
		e.log.Info("durable: resuming stalled runs", "runs", unstalled)
	}

	renewDone := make(chan struct{})
	var renewer sync.WaitGroup
	renewer.Go(func() { e.renew(renewDone) })

	slots := make(chan struct{}, e.workers)
	ticker := time.NewTicker(e.poll)
	defer ticker.Stop()
	for ctx.Err() == nil {
		e.claim(ctx, slots)
		select {
		case <-ctx.Done():
		case <-ticker.C:
		case <-e.pokes:
		}
	}

	e.drain()
	close(renewDone)
	renewer.Wait()
	return nil
}

func (e *Engine) drain() {
	e.draining.Store(true)
	done := make(chan struct{})
	go func() {
		e.runs.Wait()
		close(done)
	}()
	timer := time.NewTimer(e.grace)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		e.hardStop(errShutdown)
		<-done
	}
}

func (e *Engine) claim(ctx context.Context, slots chan struct{}) {
	free := cap(slots) - len(slots)
	if free == 0 {
		return
	}
	now := time.Now()
	runs, err := e.store.ClaimRuns(ctx, Claim{Owner: e.owner, Limit: free, Now: now, Until: now.Add(e.lease)})
	if err != nil {
		if ctx.Err() == nil {
			e.log.Error("durable: claim runs", "err", err)
		}
		return
	}
	for _, run := range runs {
		slots <- struct{}{}
		e.runs.Go(func() {
			defer func() {
				<-slots
				e.poke()
			}()
			e.execute(run)
		})
	}
}

func (e *Engine) execute(run Run) {
	ctx, cancel := context.WithCancelCause(e.hard)
	defer cancel(nil)
	e.track(run.ID, cancel)
	defer e.untrack(run.ID)
	if run.CancelRequested {
		cancel(ErrCancelled)
	}

	reg, ok := e.workflows[run.Workflow]
	if !ok {
		e.finish(run, Finish{Status: StatusStalled, Error: fmt.Sprintf("workflow %q is not registered in this binary", run.Workflow)})
		return
	}
	steps, err := e.store.Steps(ctx, run.ID)
	if err != nil {
		e.log.Error("durable: load steps", "run", run.ID, "err", err)
		e.release(run, Wake{At: time.Now().Add(5 * time.Second)})
		return
	}

	r := newRunState(ctx, e, run, steps)
	output, stopped, err := e.invoke(reg, r)
	cause := context.Cause(ctx)
	switch {
	case stopped != nil:
		e.onHalt(run, *stopped)
	case err != nil && errors.Is(cause, errShutdown):
		e.release(run, Wake{At: time.Now()})
	case err != nil && errors.Is(cause, ErrCancelled):
		e.finish(run, Finish{Status: StatusCancelled, Error: err.Error()})
	case err != nil:
		e.finish(run, Finish{Status: StatusFailed, Error: err.Error()})
	default:
		e.finish(run, Finish{Status: StatusSucceeded, Output: output})
	}
}

func (e *Engine) invoke(reg *registration, r *runState) (output []byte, stopped *halt, err error) {
	defer func() {
		p := recover()
		if r.halted != nil {
			stopped = r.halted
			return
		}
		if p == nil {
			return
		}
		e.log.Error("durable: workflow panicked", "run", r.run.ID, "workflow", r.run.Workflow, "panic", p, "stack", string(debug.Stack()))
		err = fmt.Errorf("panic: %v", p)
	}()
	output, err = reg.run(r, r.run.Input)
	return output, nil, err
}

func (e *Engine) onHalt(run Run, h halt) {
	switch h.reason {
	case haltSuspend:
		e.release(run, h.wake)
	case haltShutdown:
		e.release(run, Wake{At: time.Now()})
	case haltStore:
		e.log.Error("durable: store failed mid-run", "run", run.ID, "err", h.err)
		e.release(run, Wake{At: time.Now().Add(5 * time.Second)})
	case haltStall:
		e.log.Warn("durable: run stalled", "run", run.ID, "workflow", run.Workflow, "reason", h.err)
		e.finish(run, Finish{Status: StatusStalled, Error: h.err.Error()})
	case haltCancel:
		e.finish(run, Finish{Status: StatusCancelled, Error: ErrCancelled.Error()})
	case haltLeaseLost:
		e.log.Warn("durable: lost the lease mid-run", "run", run.ID)
	}
}

func (e *Engine) release(run Run, wake Wake) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := e.store.Release(ctx, e.owner, run.ID, wake)
	if err != nil && !errors.Is(err, ErrLeaseLost) {
		e.log.Error("durable: release run", "run", run.ID, "err", err)
	}
}

func (e *Engine) finish(run Run, finish Finish) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := e.store.FinishRun(ctx, e.owner, run.ID, finish)
	if err != nil && !errors.Is(err, ErrLeaseLost) {
		e.log.Error("durable: finish run", "run", run.ID, "err", err)
	}
	e.notifyChanged()
}

func (e *Engine) renew(done <-chan struct{}) {
	ticker := time.NewTicker(e.lease / 3)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}
		ids := e.activeIDs()
		if len(ids) == 0 {
			continue
		}
		leases, err := e.store.RenewLeases(context.Background(), e.owner, ids, time.Now().Add(e.lease))
		if err != nil {
			e.log.Error("durable: renew leases", "err", err)
			continue
		}
		for _, id := range leases.Cancelled {
			e.cancelLocal(id, ErrCancelled)
		}
		for _, id := range leases.Lost {
			e.cancelLocal(id, ErrLeaseLost)
		}
	}
}

func (e *Engine) Signal(ctx context.Context, runID, name string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := e.store.SendSignal(ctx, runID, name, data); err != nil {
		return err
	}
	e.poke()
	return nil
}

func (e *Engine) Cancel(ctx context.Context, runID string) error {
	if _, err := e.store.Cancel(ctx, runID); err != nil {
		return err
	}
	e.cancelLocal(runID, ErrCancelled)
	e.notifyChanged()
	return nil
}

func (e *Engine) Restart(ctx context.Context, runID string) error {
	if err := e.store.Restart(ctx, runID); err != nil {
		return err
	}
	e.poke()
	return nil
}

type RunInfo struct {
	Run   Run
	Steps []StepRecord
}

func (e *Engine) Get(ctx context.Context, runID string) (RunInfo, error) {
	run, err := e.store.GetRun(ctx, runID)
	if err != nil {
		return RunInfo{}, err
	}
	steps, err := e.store.Steps(ctx, runID)
	if err != nil {
		return RunInfo{}, err
	}
	return RunInfo{Run: run, Steps: steps}, nil
}

func (e *Engine) List(ctx context.Context, filter RunFilter) ([]Run, error) {
	return e.store.ListRuns(ctx, filter)
}

func (e *Engine) wait(ctx context.Context, runID string) (Run, error) {
	for {
		changed := e.changedChan()
		run, err := e.store.GetRun(ctx, runID)
		if err != nil {
			return Run{}, err
		}
		if run.Status.IsFinished() {
			return run, nil
		}
		timer := time.NewTimer(e.poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Run{}, ctx.Err()
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (e *Engine) Wake() { e.poke() }

func (e *Engine) poke() {
	select {
	case e.pokes <- struct{}{}:
	default:
	}
}

func (e *Engine) track(id string, cancel context.CancelCauseFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.active[id] = cancel
}

func (e *Engine) untrack(id string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.active, id)
}

func (e *Engine) activeIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Collect(maps.Keys(e.active))
}

func (e *Engine) cancelLocal(id string, cause error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cancel, ok := e.active[id]; ok {
		cancel(cause)
	}
}

func (e *Engine) changedChan() chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.changed
}

func (e *Engine) notifyChanged() {
	e.mu.Lock()
	defer e.mu.Unlock()
	close(e.changed)
	e.changed = make(chan struct{})
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
