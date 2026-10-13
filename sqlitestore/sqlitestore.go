package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ThallesP/durable"
)

const schema = `
CREATE TABLE IF NOT EXISTS durable_runs (
	id TEXT PRIMARY KEY,
	workflow TEXT NOT NULL,
	input BLOB,
	status TEXT NOT NULL,
	output BLOB,
	error TEXT NOT NULL DEFAULT '',
	concurrency_key TEXT NOT NULL DEFAULT '',
	concurrency_limit INTEGER NOT NULL DEFAULT 0,
	owner TEXT NOT NULL DEFAULT '',
	lease_until INTEGER NOT NULL DEFAULT 0,
	wake_at INTEGER NOT NULL DEFAULT 0,
	wait_signal TEXT NOT NULL DEFAULT '',
	cancel_requested INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	finished_at INTEGER NOT NULL DEFAULT 0,
	seq INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS durable_runs_status ON durable_runs (status, seq);
CREATE INDEX IF NOT EXISTS durable_runs_workflow ON durable_runs (workflow, seq);
CREATE TABLE IF NOT EXISTS durable_steps (
	run_id TEXT NOT NULL,
	key TEXT NOT NULL,
	name TEXT NOT NULL,
	kind TEXT NOT NULL,
	status TEXT NOT NULL,
	output BLOB,
	error TEXT NOT NULL DEFAULT '',
	timed_out INTEGER NOT NULL DEFAULT 0,
	deadline INTEGER NOT NULL DEFAULT 0,
	attempts INTEGER NOT NULL DEFAULT 0,
	started_at INTEGER NOT NULL,
	finished_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (run_id, key)
);
CREATE TABLE IF NOT EXISTS durable_signals (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id TEXT NOT NULL,
	name TEXT NOT NULL,
	payload BLOB
);
CREATE INDEX IF NOT EXISTS durable_signals_run ON durable_signals (run_id, name, seq);
`

const runColumns = `id, workflow, input, status, output, error, concurrency_key, concurrency_limit, owner,
	lease_until, wake_at, wait_signal, cancel_requested, created_at, updated_at, finished_at`

type Store struct {
	db     *sql.DB
	writes sync.Mutex
}

var _ durable.Store = (*Store)(nil)

func New(ctx context.Context, db *sql.DB) (*Store, error) {
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *Store) write(ctx context.Context, fn func(tx *sql.Tx) error) error {
	s.writes.Lock()
	defer s.writes.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateRun(ctx context.Context, run durable.Run) (durable.Run, bool, error) {
	created := false
	err := s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO durable_runs (`+runColumns+`, seq)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM durable_runs))
			ON CONFLICT (id) DO NOTHING`,
			run.ID, run.Workflow, run.Input, run.Status, run.Output, run.Error, run.ConcurrencyKey, run.ConcurrencyLimit,
			run.Owner, nanos(run.LeaseUntil), nanos(run.WakeAt), run.WaitSignal, run.CancelRequested,
			nanos(run.CreatedAt), nanos(run.UpdatedAt), nanos(run.FinishedAt))
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		created = n == 1
		return err
	})
	if err != nil {
		return durable.Run{}, false, err
	}
	got, err := s.GetRun(ctx, run.ID)
	return got, created, err
}

func (s *Store) GetRun(ctx context.Context, id string) (durable.Run, error) {
	return getRun(ctx, s.db, id)
}

func getRun(ctx context.Context, q querier, id string) (durable.Run, error) {
	runs, err := queryRuns(ctx, q, `SELECT `+runColumns+` FROM durable_runs WHERE id = ?`, id)
	if err != nil {
		return durable.Run{}, err
	}
	if len(runs) == 0 {
		return durable.Run{}, durable.ErrNotFound
	}
	return runs[0], nil
}

func (s *Store) ListRuns(ctx context.Context, filter durable.RunFilter) ([]durable.Run, error) {
	where := []string{"1 = 1"}
	args := []any{}
	if filter.Workflow != "" {
		where = append(where, "workflow = ?")
		args = append(args, filter.Workflow)
	}
	if len(filter.Statuses) > 0 {
		where = append(where, "status IN ("+placeholders(len(filter.Statuses))+")")
		for _, status := range filter.Statuses {
			args = append(args, status)
		}
	}
	limit := -1
	if filter.Limit > 0 {
		limit = filter.Limit
	}
	args = append(args, limit)
	return queryRuns(ctx, s.db, `SELECT `+runColumns+` FROM durable_runs WHERE `+strings.Join(where, " AND ")+` ORDER BY seq DESC LIMIT ?`, args...)
}

func (s *Store) ClaimRuns(ctx context.Context, claim durable.Claim) ([]durable.Run, error) {
	claimed := []durable.Run{}
	err := s.write(ctx, func(tx *sql.Tx) error {
		now := nanos(claim.Now)
		resumable, err := queryRuns(ctx, tx, `SELECT `+runColumns+` FROM durable_runs
			WHERE (status = 'waiting' AND wake_at > 0 AND wake_at <= ?) OR (status = 'running' AND lease_until < ?)
			ORDER BY seq LIMIT ?`, now, now, claim.Limit)
		if err != nil {
			return err
		}
		claimed = append(claimed, resumable...)

		held := map[string]int{}
		rows, err := tx.QueryContext(ctx, `SELECT concurrency_key, COUNT(*) FROM durable_runs
			WHERE concurrency_key != '' AND status IN ('running', 'waiting') GROUP BY concurrency_key`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var key string
			var count int
			if err := rows.Scan(&key, &count); err != nil {
				rows.Close()
				return err
			}
			held[key] = count
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		pending, err := queryRuns(ctx, tx, `SELECT `+runColumns+` FROM durable_runs WHERE status = 'pending' ORDER BY seq LIMIT 1000`)
		if err != nil {
			return err
		}
		for _, run := range pending {
			if len(claimed) >= claim.Limit {
				break
			}
			if run.ConcurrencyLimit > 0 && held[run.ConcurrencyKey] >= run.ConcurrencyLimit {
				continue
			}
			held[run.ConcurrencyKey]++
			claimed = append(claimed, run)
		}

		for i := range claimed {
			run := &claimed[i]
			run.Status = durable.StatusRunning
			run.Owner = claim.Owner
			run.LeaseUntil = claim.Until
			run.WakeAt = time.Time{}
			run.WaitSignal = ""
			run.UpdatedAt = claim.Now
			_, err := tx.ExecContext(ctx, `UPDATE durable_runs SET status = 'running', owner = ?, lease_until = ?, wake_at = 0,
				wait_signal = '', updated_at = ? WHERE id = ?`, claim.Owner, nanos(claim.Until), now, run.ID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return claimed, err
}

func (s *Store) RenewLeases(ctx context.Context, owner string, ids []string, until time.Time) (durable.Leases, error) {
	leases := durable.Leases{}
	err := s.write(ctx, func(tx *sql.Tx) error {
		for _, id := range ids {
			var cancelRequested bool
			err := tx.QueryRowContext(ctx, `UPDATE durable_runs SET lease_until = ?
				WHERE id = ? AND owner = ? AND status = 'running' RETURNING cancel_requested`, nanos(until), id, owner).Scan(&cancelRequested)
			if errors.Is(err, sql.ErrNoRows) {
				leases.Lost = append(leases.Lost, id)
				continue
			}
			if err != nil {
				return err
			}
			if cancelRequested {
				leases.Cancelled = append(leases.Cancelled, id)
			}
		}
		return nil
	})
	return leases, err
}

func (s *Store) Steps(ctx context.Context, runID string) ([]durable.StepRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT key, name, kind, status, output, error, timed_out, deadline, attempts, started_at, finished_at
		FROM durable_steps WHERE run_id = ? ORDER BY rowid`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	steps := []durable.StepRecord{}
	for rows.Next() {
		var step durable.StepRecord
		var deadline, started, finished int64
		err := rows.Scan(&step.Key, &step.Name, &step.Kind, &step.Status, &step.Output, &step.Error, &step.TimedOut,
			&deadline, &step.Attempts, &started, &finished)
		if err != nil {
			return nil, err
		}
		step.Deadline = fromNanos(deadline)
		step.StartedAt = fromNanos(started)
		step.FinishedAt = fromNanos(finished)
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func (s *Store) SaveStep(ctx context.Context, owner, runID string, step durable.StepRecord) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		if err := checkOwner(ctx, tx, owner, runID); err != nil {
			return err
		}
		return upsertStep(ctx, tx, runID, step)
	})
}

func (s *Store) ConsumeSignal(ctx context.Context, owner, runID, name string, step durable.StepRecord) (durable.StepRecord, bool, error) {
	consumed := false
	err := s.write(ctx, func(tx *sql.Tx) error {
		if err := checkOwner(ctx, tx, owner, runID); err != nil {
			return err
		}
		var seq int64
		err := tx.QueryRowContext(ctx, `SELECT seq, payload FROM durable_signals WHERE run_id = ? AND name = ? ORDER BY seq LIMIT 1`,
			runID, name).Scan(&seq, &step.Output)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM durable_signals WHERE seq = ?`, seq); err != nil {
			return err
		}
		consumed = true
		return upsertStep(ctx, tx, runID, step)
	})
	if err != nil || !consumed {
		return durable.StepRecord{}, false, err
	}
	return step, true, nil
}

func (s *Store) Release(ctx context.Context, owner, runID string, wake durable.Wake) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		if err := checkOwner(ctx, tx, owner, runID); err != nil {
			return err
		}
		now := time.Now()
		wakeAt := wake.At
		if wake.Signal != "" {
			var pending bool
			err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM durable_signals WHERE run_id = ? AND name = ?)`, runID, wake.Signal).Scan(&pending)
			if err != nil {
				return err
			}
			if pending {
				wakeAt = now
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE durable_runs SET status = 'waiting', owner = '', lease_until = 0, wake_at = ?,
			wait_signal = ?, updated_at = ? WHERE id = ?`, nanos(wakeAt), wake.Signal, nanos(now), runID)
		return err
	})
}

func (s *Store) FinishRun(ctx context.Context, owner, runID string, finish durable.Finish) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		if err := checkOwner(ctx, tx, owner, runID); err != nil {
			return err
		}
		now := time.Now()
		finished := int64(0)
		if finish.Status.IsFinished() {
			finished = nanos(now)
			if _, err := tx.ExecContext(ctx, `DELETE FROM durable_signals WHERE run_id = ?`, runID); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE durable_runs SET status = ?, output = ?, error = ?, owner = '', lease_until = 0,
			wake_at = 0, wait_signal = '', updated_at = ?, finished_at = ? WHERE id = ?`,
			finish.Status, finish.Output, finish.Error, nanos(now), finished, runID)
		return err
	})
}

func (s *Store) SendSignal(ctx context.Context, runID, name string, payload []byte) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		run, err := getRun(ctx, tx, runID)
		if err != nil {
			return err
		}
		if run.Status.IsFinished() {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO durable_signals (run_id, name, payload) VALUES (?, ?, ?)`, runID, name, payload); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE durable_runs SET wake_at = ? WHERE id = ? AND status = 'waiting' AND wait_signal = ?`,
			nanos(time.Now()), runID, name)
		return err
	})
}

func (s *Store) Cancel(ctx context.Context, runID string) (durable.Run, error) {
	var run durable.Run
	err := s.write(ctx, func(tx *sql.Tx) error {
		var err error
		run, err = getRun(ctx, tx, runID)
		if err != nil || run.Status.IsFinished() {
			return err
		}
		now := nanos(time.Now())
		if run.Status == durable.StatusRunning {
			_, err = tx.ExecContext(ctx, `UPDATE durable_runs SET cancel_requested = 1, updated_at = ? WHERE id = ?`, now, runID)
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM durable_signals WHERE run_id = ?`, runID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE durable_runs SET status = 'cancelled', error = ?, wake_at = 0, wait_signal = '',
			updated_at = ?, finished_at = ? WHERE id = ?`, durable.ErrCancelled.Error(), now, now, runID)
		return err
	})
	if err != nil {
		return durable.Run{}, err
	}
	return s.GetRun(ctx, runID)
}

func (s *Store) Restart(ctx context.Context, runID string) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		run, err := getRun(ctx, tx, runID)
		if err != nil {
			return err
		}
		if !run.Status.IsFinished() && run.Status != durable.StatusStalled {
			return durable.ErrRunActive
		}
		for _, table := range []string{"durable_steps", "durable_signals"} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE run_id = ?`, runID); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE durable_runs SET status = 'pending', output = NULL, error = '', owner = '', lease_until = 0,
			wake_at = 0, wait_signal = '', cancel_requested = 0, updated_at = ?, finished_at = 0 WHERE id = ?`, nanos(time.Now()), runID)
		return err
	})
}

func (s *Store) Unstall(ctx context.Context, workflows []string) (int, error) {
	if len(workflows) == 0 {
		return 0, nil
	}
	count := 0
	err := s.write(ctx, func(tx *sql.Tx) error {
		now := nanos(time.Now())
		args := []any{now, now}
		for _, workflow := range workflows {
			args = append(args, workflow)
		}
		res, err := tx.ExecContext(ctx, `UPDATE durable_runs SET status = 'waiting', error = '', wake_at = ?, updated_at = ?
			WHERE status = 'stalled' AND workflow IN (`+placeholders(len(workflows))+`)`, args...)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		count = int(n)
		return err
	})
	return count, err
}

func checkOwner(ctx context.Context, tx *sql.Tx, owner, runID string) error {
	var ok bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM durable_runs WHERE id = ? AND owner = ? AND status = 'running')`,
		runID, owner).Scan(&ok)
	if err != nil {
		return err
	}
	if !ok {
		return durable.ErrLeaseLost
	}
	return nil
}

func upsertStep(ctx context.Context, tx *sql.Tx, runID string, step durable.StepRecord) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO durable_steps
		(run_id, key, name, kind, status, output, error, timed_out, deadline, attempts, started_at, finished_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (run_id, key) DO UPDATE SET name = excluded.name, kind = excluded.kind, status = excluded.status,
			output = excluded.output, error = excluded.error, timed_out = excluded.timed_out, deadline = excluded.deadline,
			attempts = excluded.attempts, started_at = excluded.started_at, finished_at = excluded.finished_at`,
		runID, step.Key, step.Name, step.Kind, step.Status, step.Output, step.Error, step.TimedOut, nanos(step.Deadline),
		step.Attempts, nanos(step.StartedAt), nanos(step.FinishedAt))
	return err
}

func queryRuns(ctx context.Context, q querier, query string, args ...any) ([]durable.Run, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []durable.Run{}
	for rows.Next() {
		var run durable.Run
		var lease, wake, created, updated, finished int64
		err := rows.Scan(&run.ID, &run.Workflow, &run.Input, &run.Status, &run.Output, &run.Error, &run.ConcurrencyKey,
			&run.ConcurrencyLimit, &run.Owner, &lease, &wake, &run.WaitSignal, &run.CancelRequested, &created, &updated, &finished)
		if err != nil {
			return nil, err
		}
		run.LeaseUntil = fromNanos(lease)
		run.WakeAt = fromNanos(wake)
		run.CreatedAt = fromNanos(created)
		run.UpdatedAt = fromNanos(updated)
		run.FinishedAt = fromNanos(finished)
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func nanos(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func fromNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
