# durable

Durable workflows for Go with persistence you choose. A workflow is a plain Go function. Each step's
result is saved, so after a crash, a restart or a deploy the function runs again from the top, and
steps that already finished return their saved result instead of running again.

There's no server and no Postgres requirement. Ships with `memstore` (tests) and `sqlitestore`
(any `*sql.DB` on SQLite). Any other database is one `Store` implementation away, checked by a shared
conformance suite.

```go
store, err := sqlitestore.New(ctx, db)
eng := durable.New(store, durable.Workers(16), durable.Logger(log))
deploy := durable.Register(eng, "deploy", app.runDeploy,
	durable.Concurrency(1), durable.PartitionBy(func(in DeployInput) string { return in.EnvironmentID }))
go eng.Run(ctx)

func (a *App) runDeploy(ctx durable.Context, in DeployInput) (DeployResult, error) {
	plan, err := durable.Step(ctx, "plan", func(ctx context.Context) (Plan, error) {
		return a.planDeployment(ctx, in.DeploymentID)
	})
	if err != nil {
		return DeployResult{}, err
	}

	for _, svc := range plan.Services {
		err := durable.Do(ctx, "apply "+svc.Name, func(ctx context.Context) error {
			return a.swarm.Apply(ctx, svc)
		}, durable.Retry(5, 2*time.Second))
		if err != nil {
			return DeployResult{}, err
		}
	}

	report, err := durable.Await[Converged](ctx, "converged", 10*time.Minute)
	if errors.Is(err, durable.ErrTimeout) {
		return DeployResult{}, a.failDeployment(ctx, in.DeploymentID, "timed out")
	}
	return DeployResult{Nodes: report.Nodes}, err
}
```

```go
run, err := deploy.Start(ctx, in, durable.WithID("deploy:"+id))  // same ID returns the same run
res, err := run.Result(ctx)
err = eng.Signal(ctx, "deploy:"+id, "converged", Converged{...})
err = eng.Cancel(ctx, "deploy:"+id)
err = eng.Restart(ctx, "deploy:"+id)                              // stalled, failed or cancelled runs
info, err := eng.Get(ctx, "deploy:"+id)                           // run plus every step, for a UI
```

## API

| Call | What it does |
|---|---|
| `durable.Step[T](ctx, name, fn, opts...)` | Runs `fn` once, saves its result as JSON, returns the saved result on replay. |
| `durable.Do(ctx, name, fn, opts...)` | A step without a result. |
| `durable.Sleep(ctx, name, d)` | A sleep that survives restarts: the deadline is saved, the run is parked and resumes when it's due. |
| `durable.Await[T](ctx, name, timeout)` | Waits for `Engine.Signal(runID, name, payload)`. `timeout` 0 waits forever; on timeout it returns an error wrapping `ErrTimeout`. Signals sent before the run reaches the await are kept. |
| `durable.Now(ctx)` | The time, saved, so replay sees the same value. |
| `durable.Retry(attempts, backoff)`, `durable.Timeout(d)`, `durable.Permanent(err)` | Step options; `Permanent` stops retrying. |
| `durable.Concurrency(n)`, `durable.PartitionBy(fn)` | At most `n` runs of the workflow at a time, per partition. A run holds its slot while it runs or waits. |

A failed step returns a `*durable.StepError`. The failure is saved like a result, so replay returns
the same error (the message survives; `errors.Is` on the original error only works in the run that
hit it).

## How runs execute

- Runs are claimed with a lease (`durable.Lease`, 30s default) that the engine renews. A run whose
  engine died is claimed again when its lease expires; writes from an engine that lost the lease are
  rejected.
- `Sleep` and `Await` park the run (`waiting`) and free the worker. A signal or the deadline makes it
  due again.
- On shutdown (`Run`'s context cancelled) the engine stops claiming, lets running steps finish for
  `durable.ShutdownGrace` (10s default), then cancels them. Unfinished runs are released so the next
  process picks them up at once. A step cut off this way is not saved and runs again: steps run at
  least once, so they must be idempotent.
- Statuses: `pending`, `running`, `waiting`, `stalled`, `succeeded`, `failed`, `cancelled`.

## Rules for workflow code

`durablecheck` enforces these.

- Everything outside steps must do the same thing on every replay: no `time.Now`, `time.Sleep`,
  randomness, goroutines, channels or `select` in the workflow body. Use `durable.Now`, `durable.Sleep`,
  `durable.Await`, or put it in a step.
- Step names start with a constant (`"apply " + svc.Name`), never a timestamp or a random value.
  Saved results are matched by name (plus a counter when a name repeats), not by position.
- Don't range over a map around durable calls; sort the keys.
- Steps don't call durable functions.

## Upgrades

Saved results are matched by step name, so most code changes are safe for runs that are in flight
when the new binary starts:

| Change | Runs in flight |
|---|---|
| Add a step | Run it when they reach it. It can't rely on a step after it. |
| Remove a step | Ignore its saved result. |
| Reorder steps | Fine. |
| Change a step's body | Finished steps keep their result; the rest use the new code. |
| Rename a step | Run it again. |
| Add a field to a result, input or signal | Old data decodes with the zero value. |
| Remove, rename or retype a field | Old data no longer decodes: the run **stalls**. |
| Turn a step into a sleep or await (or back) | The run stalls. |

Saved data is decoded strictly (unknown fields are errors), so a breaking change stalls the run
instead of carrying on with zero values. A stalled run keeps its error; fix the code and it resumes on
the next start, or `Restart` it. A run whose workflow isn't registered also stalls, and resumes when a
binary registers the name again.

For a rewrite, register the new code under a new name and keep the old one until its runs finish:

```go
durable.Register(eng, "deploy-v2", a.runDeploy)
durable.Register(eng, "deploy", a.runDeployV1)
```

## durablecheck

```sh
go run github.com/ThallesP/durable/cmd/durablecheck ./...          # CI: lint + compare with durable.lock
go run github.com/ThallesP/durable/cmd/durablecheck -update ./...  # record compatible changes
```

It lints workflow code for the rules above and keeps `durable.lock`: every registered workflow, its
input type and each step's name, kind and result type. CI fails when the lock is out of date, so every
change to a workflow shows up in review. `-update` records compatible changes and refuses breaking
ones; `-update -force` records them anyway, for when no run of that workflow can be in flight. The
analyzer is also exported as `check.Analyzer` for `go vet -vettool` or golangci-lint.

The skill in `skills/durable-workflows` tells an agent how to handle each finding.

## Writing a store

Implement `durable.Store` and run the conformance suite:

```go
func TestStore(t *testing.T) {
	durabletest.Store(t, func(t *testing.T) durable.Store { return mystore.New(t) })
}
```

The contract, in short: `CreateRun` is idempotent on ID. `ClaimRuns` takes waiting runs that are due
and running runs whose lease expired, then pending runs in creation order, skipping a pending run when
its `ConcurrencyKey` already has `ConcurrencyLimit` runs running or waiting. Every write made on
behalf of a run (`SaveStep`, `ConsumeSignal`, `Release`, `FinishRun`) fails with `ErrLeaseLost`
unless that owner holds the run. `ConsumeSignal` pops the oldest signal with that name and saves the
step in one transaction. `Release` and `SendSignal` wake a run waiting on a signal that's there.

## Community

Questions, ideas or just want to talk? Come say hi on [Discord](https://discord.gg/pF5wfWZMSH).
