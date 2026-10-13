package durable

import (
	"context"
	"errors"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusWaiting   Status = "waiting"
	StatusStalled   Status = "stalled"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

func (s Status) IsFinished() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCancelled
}

func (s Status) HoldsSlot() bool {
	return s == StatusRunning || s == StatusWaiting
}

type StepKind string

const (
	KindStep  StepKind = "step"
	KindSleep StepKind = "sleep"
	KindAwait StepKind = "await"
)

type StepStatus string

const (
	StepPending StepStatus = "pending"
	StepDone    StepStatus = "done"
)

type Run struct {
	ID               string
	Workflow         string
	Input            []byte
	Status           Status
	Output           []byte
	Error            string
	ConcurrencyKey   string
	ConcurrencyLimit int
	Owner            string
	LeaseUntil       time.Time
	WakeAt           time.Time
	WaitSignal       string
	CancelRequested  bool
	CreatedAt        time.Time
	UpdatedAt        time.Time
	FinishedAt       time.Time
}

type StepRecord struct {
	Key        string
	Name       string
	Kind       StepKind
	Status     StepStatus
	Output     []byte
	Error      string
	TimedOut   bool
	Deadline   time.Time
	Attempts   int
	StartedAt  time.Time
	FinishedAt time.Time
}

type Claim struct {
	Owner string
	Limit int
	Now   time.Time
	Until time.Time
}

type Wake struct {
	At     time.Time
	Signal string
}

type Finish struct {
	Status Status
	Output []byte
	Error  string
}

type Leases struct {
	Cancelled []string
	Lost      []string
}

type RunFilter struct {
	Workflow string
	Statuses []Status
	Limit    int
}

var (
	ErrNotFound    = errors.New("durable: run not found")
	ErrLeaseLost   = errors.New("durable: run is no longer owned by this engine")
	ErrRunActive   = errors.New("durable: run is still active")
	ErrIDConflict  = errors.New("durable: run id belongs to another workflow")
	ErrTimeout     = errors.New("durable: timed out")
	ErrCancelled   = errors.New("durable: run cancelled")
	ErrEngineState = errors.New("durable: engine already running")
)

type Store interface {
	CreateRun(ctx context.Context, run Run) (Run, bool, error)
	GetRun(ctx context.Context, id string) (Run, error)
	ListRuns(ctx context.Context, filter RunFilter) ([]Run, error)
	ClaimRuns(ctx context.Context, claim Claim) ([]Run, error)
	RenewLeases(ctx context.Context, owner string, ids []string, until time.Time) (Leases, error)
	Steps(ctx context.Context, runID string) ([]StepRecord, error)
	SaveStep(ctx context.Context, owner, runID string, step StepRecord) error
	ConsumeSignal(ctx context.Context, owner, runID, signal string, step StepRecord) (StepRecord, bool, error)
	Release(ctx context.Context, owner, runID string, wake Wake) error
	FinishRun(ctx context.Context, owner, runID string, finish Finish) error
	SendSignal(ctx context.Context, runID, name string, payload []byte) error
	Cancel(ctx context.Context, runID string) (Run, error)
	Restart(ctx context.Context, runID string) error
	Unstall(ctx context.Context, workflows []string) (int, error)
}
