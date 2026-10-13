package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

type registration struct {
	name          string
	limit         int
	partition     func(any) string
	partitionType reflect.Type
	run           func(r *runState, input []byte) ([]byte, error)
}

type RegisterOption func(*registration)

func Concurrency(n int) RegisterOption {
	return func(reg *registration) { reg.limit = n }
}

func PartitionBy[I any](fn func(I) string) RegisterOption {
	return func(reg *registration) {
		reg.partitionType = reflect.TypeFor[I]()
		reg.partition = func(in any) string { return fn(in.(I)) }
	}
}

type Workflow[I, O any] struct {
	engine *Engine
	reg    *registration
}

func Register[I, O any](e *Engine, name string, fn func(Context, I) (O, error), opts ...RegisterOption) *Workflow[I, O] {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started {
		panic(fmt.Sprintf("durable: register %q before Engine.Run", name))
	}
	if name == "" {
		panic("durable: workflow name is empty")
	}
	if _, ok := e.workflows[name]; ok {
		panic(fmt.Sprintf("durable: workflow %q is registered twice", name))
	}

	reg := &registration{name: name}
	for _, opt := range opts {
		opt(reg)
	}
	if reg.partition != nil && reg.partitionType != reflect.TypeFor[I]() {
		panic(fmt.Sprintf("durable: workflow %q takes %s but PartitionBy reads %s", name, reflect.TypeFor[I](), reg.partitionType))
	}
	if reg.partition != nil && reg.limit == 0 {
		panic(fmt.Sprintf("durable: workflow %q uses PartitionBy without Concurrency", name))
	}
	reg.run = func(r *runState, input []byte) ([]byte, error) {
		var in I
		if err := decodeStrict(input, &in); err != nil {
			r.stall("the saved input no longer decodes into %T: %v", in, err)
		}
		out, err := fn(r, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}
	e.workflows[name] = reg
	return &Workflow[I, O]{engine: e, reg: reg}
}

type StartOption func(*Run)

func WithID(id string) StartOption {
	return func(run *Run) { run.ID = id }
}

func (w *Workflow[I, O]) Name() string { return w.reg.name }

func (w *Workflow[I, O]) Start(ctx context.Context, in I, opts ...StartOption) (*Handle[O], error) {
	run, err := w.NewRun(in, opts...)
	if err != nil {
		return nil, err
	}
	got, _, err := w.engine.store.CreateRun(ctx, run)
	if err != nil {
		return nil, err
	}
	if got.Workflow != run.Workflow {
		return nil, fmt.Errorf("%w: %s is a %s run", ErrIDConflict, got.ID, got.Workflow)
	}
	w.engine.poke()
	return w.Handle(got.ID), nil
}

func (w *Workflow[I, O]) NewRun(in I, opts ...StartOption) (Run, error) {
	input, err := json.Marshal(in)
	if err != nil {
		return Run{}, err
	}
	now := time.Now()
	run := Run{
		ID:               newID(),
		Workflow:         w.reg.name,
		Input:            input,
		Status:           StatusPending,
		ConcurrencyLimit: w.reg.limit,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if w.reg.limit > 0 {
		run.ConcurrencyKey = w.reg.name
	}
	if w.reg.partition != nil {
		run.ConcurrencyKey += "/" + w.reg.partition(in)
	}
	for _, opt := range opts {
		opt(&run)
	}
	return run, nil
}

func (w *Workflow[I, O]) Handle(id string) *Handle[O] {
	return &Handle[O]{engine: w.engine, id: id}
}

type Handle[O any] struct {
	engine *Engine
	id     string
}

func (h *Handle[O]) ID() string { return h.id }

func (h *Handle[O]) Result(ctx context.Context) (O, error) {
	var out O
	run, err := h.engine.wait(ctx, h.id)
	if err != nil {
		return out, err
	}
	if run.Status != StatusSucceeded {
		return out, &RunError{RunID: run.ID, Status: run.Status, Message: run.Error}
	}
	if err := json.Unmarshal(run.Output, &out); err != nil {
		return out, err
	}
	return out, nil
}

type RunError struct {
	RunID   string
	Status  Status
	Message string
}

func (e *RunError) Error() string {
	return fmt.Sprintf("run %s %s: %s", e.RunID, e.Status, e.Message)
}

func (e *RunError) Is(target error) bool {
	return target == ErrCancelled && e.Status == StatusCancelled
}
