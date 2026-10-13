package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type StepOption func(*stepConfig)

type stepConfig struct {
	attempts int
	backoff  time.Duration
	timeout  time.Duration
}

func Retry(attempts int, backoff time.Duration) StepOption {
	return func(c *stepConfig) {
		c.attempts = attempts
		c.backoff = backoff
	}
}

func Timeout(d time.Duration) StepOption {
	return func(c *stepConfig) { c.timeout = d }
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func Permanent(err error) error {
	return &permanentError{err: err}
}

type StepError struct {
	Step    string
	Message string
	cause   error
}

func (e *StepError) Error() string { return fmt.Sprintf("step %q: %s", e.Step, e.Message) }
func (e *StepError) Unwrap() error { return e.cause }

func Step[T any](ctx Context, name string, fn func(context.Context) (T, error), opts ...StepOption) (T, error) {
	return step(ctx.state(), name, false, true, fn, opts)
}

func Do(ctx Context, name string, fn func(context.Context) error, opts ...StepOption) error {
	_, err := step(ctx.state(), name, false, false, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, opts)
	return err
}

func Now(ctx Context) time.Time {
	now, _ := step(ctx.state(), "durable.now", true, true, func(context.Context) (time.Time, error) {
		return time.Now(), nil
	}, nil)
	return now
}

func step[T any](r *runState, name string, reserved, typed bool, fn func(context.Context) (T, error), opts []StepOption) (T, error) {
	var out T
	r.enter("step", name)
	key := r.key(name, reserved)
	if saved, ok := r.done[key]; ok {
		r.expectKind(saved, KindStep)
		if saved.Error != "" {
			return out, &StepError{Step: name, Message: saved.Error}
		}
		if !typed {
			return out, nil
		}
		if err := decodeStrict(saved.Output, &out); err != nil {
			r.stall("step %q: its saved result no longer decodes into %T: %v", key, out, err)
		}
		return out, nil
	}

	cfg := stepConfig{attempts: 1}
	for _, opt := range opts {
		opt(&cfg)
	}
	started := time.Now()
	value, attempts, err := attempt(r, fn, cfg)
	record := StepRecord{Key: key, Name: name, Kind: KindStep, Status: StepDone, Attempts: attempts, StartedAt: started}
	if err != nil {
		r.stopIfAsked()
		return out, r.saveFailure(record, err)
	}
	if typed {
		record.Output, err = json.Marshal(value)
		if err == nil {
			err = decodeStrict(record.Output, &out)
		}
		if err != nil {
			return out, r.saveFailure(record, fmt.Errorf("result can't be saved and replayed: %w", err))
		}
	}
	record.FinishedAt = time.Now()
	r.save(record)
	return out, nil
}

func (r *runState) saveFailure(record StepRecord, err error) error {
	record.Error = err.Error()
	record.FinishedAt = time.Now()
	r.save(record)
	return &StepError{Step: record.Name, Message: record.Error, cause: err}
}

func attempt[T any](r *runState, fn func(context.Context) (T, error), cfg stepConfig) (T, int, error) {
	delay := cfg.backoff
	for n := 1; ; n++ {
		value, err := call(r, fn, cfg.timeout)
		var permanent *permanentError
		if errors.As(err, &permanent) {
			return value, n, permanent.err
		}
		if err == nil || n >= cfg.attempts || r.Err() != nil {
			return value, n, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-r.Done():
			timer.Stop()
			return value, n, err
		}
		delay *= 2
	}
}

func call[T any](r *runState, fn func(context.Context) (T, error), timeout time.Duration) (value T, err error) {
	ctx, cancel := context.WithCancel(r.Context)
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(r.Context, timeout)
	}
	defer cancel()
	r.inStep = true
	defer func() {
		r.inStep = false
		if p := recover(); p != nil {
			err = Permanent(fmt.Errorf("panic: %v", p))
		}
	}()
	return fn(ctx)
}
