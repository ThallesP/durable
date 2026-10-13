package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Context interface {
	context.Context
	RunID() string
	Workflow() string
	state() *runState
}

type haltReason int

const (
	haltSuspend haltReason = iota
	haltStall
	haltShutdown
	haltCancel
	haltLeaseLost
	haltStore
)

type halt struct {
	reason haltReason
	wake   Wake
	err    error
}

var errShutdown = errors.New("durable: engine shutting down")

type runState struct {
	context.Context
	engine *Engine
	run    Run
	done   map[string]StepRecord
	seen   map[string]int
	inStep bool
	halted *halt
}

func newRunState(ctx context.Context, e *Engine, run Run, steps []StepRecord) *runState {
	done := make(map[string]StepRecord, len(steps))
	for _, step := range steps {
		done[step.Key] = step
	}
	return &runState{Context: ctx, engine: e, run: run, done: done, seen: map[string]int{}}
}

func (r *runState) RunID() string             { return r.run.ID }
func (r *runState) Workflow() string          { return r.run.Workflow }
func (r *runState) state() *runState          { return r }
func (r *runState) storeCtx() context.Context { return context.WithoutCancel(r.Context) }

func (r *runState) halt(h halt) {
	r.halted = &h
	panic(h)
}

func (r *runState) stall(format string, args ...any) {
	r.halt(halt{reason: haltStall, err: fmt.Errorf(format, args...)})
}

func (r *runState) enter(op, name string) {
	if r.halted != nil {
		panic(*r.halted)
	}
	if r.inStep {
		panic(fmt.Sprintf("durable: %s %q called inside a step; steps can't run durable calls", op, name))
	}
	r.stopIfAsked()
}

func (r *runState) stopIfAsked() {
	cause := context.Cause(r.Context)
	switch {
	case errors.Is(cause, ErrCancelled):
		r.halt(halt{reason: haltCancel})
	case errors.Is(cause, ErrLeaseLost):
		r.halt(halt{reason: haltLeaseLost, err: cause})
	case errors.Is(cause, errShutdown), r.engine.draining.Load():
		r.halt(halt{reason: haltShutdown})
	}
}

func (r *runState) key(name string, reserved bool) string {
	if !reserved {
		validateStepName(name)
	}
	r.seen[name]++
	if n := r.seen[name]; n > 1 {
		return fmt.Sprintf("%s#%d", name, n)
	}
	return name
}

func validateStepName(name string) {
	switch {
	case name == "":
		panic("durable: step name is empty")
	case strings.Contains(name, "#"):
		panic(fmt.Sprintf("durable: step name %q contains '#', which durable uses to count repeated names", name))
	case strings.HasPrefix(name, "durable."):
		panic(fmt.Sprintf("durable: step name %q uses the reserved prefix \"durable.\"", name))
	}
}

func (r *runState) expectKind(step StepRecord, kind StepKind) {
	if step.Kind != kind {
		r.stall("%q was a %s in this run and is a %s now; give the %s another name", step.Key, step.Kind, kind, kind)
	}
}

func (r *runState) save(step StepRecord) {
	r.storeErr(r.engine.store.SaveStep(r.storeCtx(), r.engine.owner, r.run.ID, step))
	r.done[step.Key] = step
}

func (r *runState) storeErr(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, ErrLeaseLost) {
		r.halt(halt{reason: haltLeaseLost, err: err})
	}
	r.halt(halt{reason: haltStore, err: err})
}

func (r *runState) suspend(at time.Time, signal string) {
	remaining := time.Until(at)
	if signal != "" || at.IsZero() || remaining > r.engine.poll {
		r.halt(halt{reason: haltSuspend, wake: Wake{At: at, Signal: signal}})
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.Done():
		r.stopIfAsked()
	}
}

func decodeStrict(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
