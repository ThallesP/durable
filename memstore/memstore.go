package memstore

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/ThallesP/durable"
)

type signal struct {
	name    string
	payload []byte
}

type Store struct {
	mu      sync.Mutex
	runs    map[string]*durable.Run
	order   []string
	steps   map[string][]durable.StepRecord
	signals map[string][]signal
}

var _ durable.Store = (*Store)(nil)

func New() *Store {
	return &Store{
		runs:    map[string]*durable.Run{},
		steps:   map[string][]durable.StepRecord{},
		signals: map[string][]signal{},
	}
}

func (s *Store) CreateRun(_ context.Context, run durable.Run) (durable.Run, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.runs[run.ID]; ok {
		return *existing, false, nil
	}
	s.runs[run.ID] = &run
	s.order = append(s.order, run.ID)
	return run, true, nil
}

func (s *Store) GetRun(_ context.Context, id string) (durable.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return durable.Run{}, durable.ErrNotFound
	}
	return *run, nil
}

func (s *Store) ListRuns(_ context.Context, filter durable.RunFilter) ([]durable.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runs := []durable.Run{}
	for _, id := range slices.Backward(s.order) {
		run := s.runs[id]
		if filter.Workflow != "" && run.Workflow != filter.Workflow {
			continue
		}
		if len(filter.Statuses) > 0 && !slices.Contains(filter.Statuses, run.Status) {
			continue
		}
		runs = append(runs, *run)
		if filter.Limit > 0 && len(runs) == filter.Limit {
			break
		}
	}
	return runs, nil
}

func (s *Store) ClaimRuns(_ context.Context, claim durable.Claim) ([]durable.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := map[string]int{}
	for _, run := range s.runs {
		if run.ConcurrencyKey != "" && run.Status.HoldsSlot() {
			held[run.ConcurrencyKey]++
		}
	}

	claimed := []durable.Run{}
	take := func(run *durable.Run) {
		run.Status = durable.StatusRunning
		run.Owner = claim.Owner
		run.LeaseUntil = claim.Until
		run.WakeAt = time.Time{}
		run.WaitSignal = ""
		run.UpdatedAt = claim.Now
		claimed = append(claimed, *run)
	}
	for _, id := range s.order {
		if len(claimed) == claim.Limit {
			return claimed, nil
		}
		run := s.runs[id]
		isDue := run.Status == durable.StatusWaiting && !run.WakeAt.IsZero() && !run.WakeAt.After(claim.Now)
		isAbandoned := run.Status == durable.StatusRunning && run.LeaseUntil.Before(claim.Now)
		if isDue || isAbandoned {
			take(run)
		}
	}
	for _, id := range s.order {
		if len(claimed) == claim.Limit {
			return claimed, nil
		}
		run := s.runs[id]
		if run.Status != durable.StatusPending {
			continue
		}
		if run.ConcurrencyLimit > 0 && held[run.ConcurrencyKey] >= run.ConcurrencyLimit {
			continue
		}
		held[run.ConcurrencyKey]++
		take(run)
	}
	return claimed, nil
}

func (s *Store) RenewLeases(_ context.Context, owner string, ids []string, until time.Time) (durable.Leases, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	leases := durable.Leases{}
	for _, id := range ids {
		run, ok := s.owned(owner, id)
		if !ok {
			leases.Lost = append(leases.Lost, id)
			continue
		}
		run.LeaseUntil = until
		if run.CancelRequested {
			leases.Cancelled = append(leases.Cancelled, id)
		}
	}
	return leases, nil
}

func (s *Store) Steps(_ context.Context, runID string) ([]durable.StepRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.steps[runID]), nil
}

func (s *Store) SaveStep(_ context.Context, owner, runID string, step durable.StepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.owned(owner, runID); !ok {
		return durable.ErrLeaseLost
	}
	s.upsertStep(runID, step)
	return nil
}

func (s *Store) ConsumeSignal(_ context.Context, owner, runID, name string, step durable.StepRecord) (durable.StepRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.owned(owner, runID); !ok {
		return durable.StepRecord{}, false, durable.ErrLeaseLost
	}
	i := slices.IndexFunc(s.signals[runID], func(sig signal) bool { return sig.name == name })
	if i < 0 {
		return durable.StepRecord{}, false, nil
	}
	step.Output = s.signals[runID][i].payload
	s.signals[runID] = slices.Delete(s.signals[runID], i, i+1)
	s.upsertStep(runID, step)
	return step, true, nil
}

func (s *Store) Release(_ context.Context, owner, runID string, wake durable.Wake) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.owned(owner, runID)
	if !ok {
		return durable.ErrLeaseLost
	}
	run.Status = durable.StatusWaiting
	run.Owner = ""
	run.LeaseUntil = time.Time{}
	run.WakeAt = wake.At
	run.WaitSignal = wake.Signal
	run.UpdatedAt = time.Now()
	if wake.Signal != "" && s.hasSignal(runID, wake.Signal) {
		run.WakeAt = run.UpdatedAt
	}
	return nil
}

func (s *Store) FinishRun(_ context.Context, owner, runID string, finish durable.Finish) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.owned(owner, runID)
	if !ok {
		return durable.ErrLeaseLost
	}
	now := time.Now()
	run.Status = finish.Status
	run.Output = finish.Output
	run.Error = finish.Error
	run.Owner = ""
	run.LeaseUntil = time.Time{}
	run.WakeAt = time.Time{}
	run.WaitSignal = ""
	run.UpdatedAt = now
	if finish.Status.IsFinished() {
		run.FinishedAt = now
		delete(s.signals, runID)
	}
	return nil
}

func (s *Store) SendSignal(_ context.Context, runID, name string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return durable.ErrNotFound
	}
	if run.Status.IsFinished() {
		return nil
	}
	s.signals[runID] = append(s.signals[runID], signal{name: name, payload: payload})
	if run.Status == durable.StatusWaiting && run.WaitSignal == name {
		run.WakeAt = time.Now()
	}
	return nil
}

func (s *Store) Cancel(_ context.Context, runID string) (durable.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return durable.Run{}, durable.ErrNotFound
	}
	if run.Status.IsFinished() {
		return *run, nil
	}
	now := time.Now()
	run.UpdatedAt = now
	if run.Status == durable.StatusRunning {
		run.CancelRequested = true
		return *run, nil
	}
	run.Status = durable.StatusCancelled
	run.Error = durable.ErrCancelled.Error()
	run.WakeAt = time.Time{}
	run.WaitSignal = ""
	run.FinishedAt = now
	delete(s.signals, runID)
	return *run, nil
}

func (s *Store) Restart(_ context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return durable.ErrNotFound
	}
	if !run.Status.IsFinished() && run.Status != durable.StatusStalled {
		return durable.ErrRunActive
	}
	*run = durable.Run{
		ID:               run.ID,
		Workflow:         run.Workflow,
		Input:            run.Input,
		Status:           durable.StatusPending,
		ConcurrencyKey:   run.ConcurrencyKey,
		ConcurrencyLimit: run.ConcurrencyLimit,
		CreatedAt:        run.CreatedAt,
		UpdatedAt:        time.Now(),
	}
	delete(s.steps, runID)
	delete(s.signals, runID)
	return nil
}

func (s *Store) Unstall(_ context.Context, workflows []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	count := 0
	for _, run := range s.runs {
		if run.Status != durable.StatusStalled || !slices.Contains(workflows, run.Workflow) {
			continue
		}
		run.Status = durable.StatusWaiting
		run.Error = ""
		run.WakeAt = now
		run.UpdatedAt = now
		count++
	}
	return count, nil
}

func (s *Store) owned(owner, runID string) (*durable.Run, bool) {
	run, ok := s.runs[runID]
	if !ok || run.Status != durable.StatusRunning || run.Owner != owner {
		return nil, false
	}
	return run, true
}

func (s *Store) hasSignal(runID, name string) bool {
	return slices.ContainsFunc(s.signals[runID], func(sig signal) bool { return sig.name == name })
}

func (s *Store) upsertStep(runID string, step durable.StepRecord) {
	steps := s.steps[runID]
	i := slices.IndexFunc(steps, func(saved durable.StepRecord) bool { return saved.Key == step.Key })
	if i < 0 {
		s.steps[runID] = append(steps, step)
		return
	}
	steps[i] = step
}
