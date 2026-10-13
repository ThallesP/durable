package durable

import (
	"fmt"
	"time"
)

func Sleep(ctx Context, name string, d time.Duration) {
	r := ctx.state()
	r.enter("sleep", name)
	key := r.key(name, false)
	saved, ok := r.done[key]
	if !ok {
		now := time.Now()
		saved = StepRecord{Key: key, Name: name, Kind: KindSleep, Status: StepPending, Deadline: now.Add(d), StartedAt: now}
		r.save(saved)
	}
	r.expectKind(saved, KindSleep)
	if saved.Status == StepDone {
		return
	}

	r.suspend(saved.Deadline, "")
	saved.Status = StepDone
	saved.FinishedAt = time.Now()
	r.save(saved)
}

func Await[T any](ctx Context, name string, timeout time.Duration) (T, error) {
	var out T
	r := ctx.state()
	r.enter("await", name)
	key := r.key(name, false)
	saved, ok := r.done[key]
	if !ok {
		now := time.Now()
		saved = StepRecord{Key: key, Name: name, Kind: KindAwait, Status: StepPending, StartedAt: now}
		if timeout > 0 {
			saved.Deadline = now.Add(timeout)
		}
		r.save(saved)
	}
	r.expectKind(saved, KindAwait)
	if saved.Status == StepPending {
		saved = r.consume(name, saved)
	}
	if saved.Status == StepPending && !saved.Deadline.IsZero() && !time.Now().Before(saved.Deadline) {
		saved.Status = StepDone
		saved.TimedOut = true
		saved.FinishedAt = time.Now()
		r.save(saved)
	}
	if saved.Status == StepPending {
		r.suspend(saved.Deadline, name)
	}

	if saved.TimedOut {
		return out, fmt.Errorf("await %q: %w", name, ErrTimeout)
	}
	if err := decodeStrict(saved.Output, &out); err != nil {
		r.stall("await %q: its signal no longer decodes into %T: %v", key, out, err)
	}
	return out, nil
}

func (r *runState) consume(signal string, pending StepRecord) StepRecord {
	done := pending
	done.Status = StepDone
	done.FinishedAt = time.Now()
	got, ok, err := r.engine.store.ConsumeSignal(r.storeCtx(), r.engine.owner, r.run.ID, signal, done)
	r.storeErr(err)
	if !ok {
		return pending
	}
	r.done[got.Key] = got
	return got
}
