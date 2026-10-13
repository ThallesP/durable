---
name: durable-workflows
description: Write and change workflows built on github.com/ThallesP/durable (durable.Step, durable.Do, durable.Sleep, durable.Await, durable.Register) without breaking runs that are in flight when the new code ships. Use when adding or editing a workflow, when `durablecheck` fails in CI or locally, when durable.lock changes, when a run is `stalled`, or when removing an old workflow version.
---

# Durable workflows

A workflow is replayed from the top after every restart, deploy and wait. Steps that already
finished return the result saved in the store; everything else runs again. Saved results are matched
by **step name** (plus `#2`, `#3` when a name repeats), and decoded **strictly** into the current Go
type. Keep both facts in mind for every change.

`durablecheck` (lint plus `durable.lock`) is the gate. Run it after every workflow change:

```sh
go run github.com/ThallesP/durable/cmd/durablecheck ./...
```

## Writing workflow code

The lint reports each of these; fix them in the code, never by silencing the check.

| Finding | Fix |
|---|---|
| `time.Now` / `time.Since` in a workflow | `durable.Now(ctx)`, or compute it inside the step that needs it |
| `time.Sleep`, timers, tickers | `durable.Sleep(ctx, "name", d)` |
| channels, `select`, goroutines | `durable.Await` + `Engine.Signal`; parallel work goes inside one step or into separate runs |
| randomness, IDs | generate inside a `durable.Step` so the value is saved |
| ranging over a map around durable calls | `for _, k := range slices.Sorted(maps.Keys(m))` |
| step name doesn't start with a constant | `"apply " + svc.Name`, `fmt.Sprintf("apply %s", svc.Name)`; never a timestamp, counter or random value |
| durable call inside a step | move it to the workflow body; a step is one side effect |

Also, which the lint can't see:

- **Steps run at least once.** A crash after the side effect and before the save runs it again.
  Make each step idempotent: upserts, deterministic IDs from the input, "already applied" checks.
- **One side effect per step.** Two writes in one step means a retry repeats both.
- Step results, inputs and signal payloads are JSON. Use exported fields; unexported ones are lost.
- Workflow names are constant strings. A run finds its code by that name forever.

## When durablecheck fails

Read every line it prints. Each is one of these:

### `new workflow`, `new` step, `removed` step, `changed compatibly`, `step became do`

Compatible. Check the semantics the tool can't:

- **New step:** runs that are in flight reach it with only what earlier steps produced. If it reads
  something a later step makes, or assumes a step that already ran under the old code did something
  new, it's not safe: guard it, or treat the change as breaking (below).
- **Removed + new step with similar names:** that's a rename. Runs that passed the old name run the
  step again. Fine if it's idempotent; if not, keep the old name.
- **Changed step body:** runs that already finished that step keep the old behaviour's result. Usually
  fine; the next run picks up the new behaviour.

Then record it: `durablecheck -update`, and commit `durable.lock` with the code.

### `no longer registered`

A workflow was deleted. Runs of it that are still in flight will stall. Only delete a workflow after
confirming none of its runs are `pending`, `running`, `waiting` or `stalled` in every deployment that
runs this code (`Engine.List` with those statuses, or the app's own admin view). Then `-update`.

### `BREAKING`

Runs in flight would stall (or, for `do became step`, get a zero value). `-update` refuses it. Pick
the first option that fits:

1. **Keep the old shape.** Add a new field instead of renaming or retyping one; keep the old field and
   stop writing it. Widening `int` to `float64` and adding fields are compatible.
2. **Rename the step.** `"plan"` becomes `"plan v2"`: runs in flight ignore the old saved result and run
   the step again with the new type. Only when that step is idempotent and cheap to repeat.
3. **New workflow name.** Copy the current function to `runXV1` in a `legacy_<name>.go` file, register
   it under the old name, and register the new code under a new name:

   ```go
   durable.Register(eng, "deploy-v2", a.runDeploy)
   durable.Register(eng, "deploy", a.runDeployV1)
   ```

   Change every `Start` call to the new workflow. The legacy function must keep compiling against the
   old types; copy those types into the legacy file too if they change. Delete it in a later release,
   following "no longer registered" above.
4. **`-update -force`.** Only when no run of that workflow can be in flight anywhere: the workflow has
   never shipped, or you checked every deployment as above. Say which in the commit message.

Never edit `durable.lock` by hand, and never pick 4 to make CI pass.

## Stalled runs

A run stalls when its saved data no longer decodes, a step changed kind, or its workflow isn't
registered. Its `Error` names the step and field. It resumes by itself when a binary that can decode it
starts (the fix shipped, or a rollback). Otherwise `Engine.Restart(runID)` runs it again from scratch,
or `Engine.Cancel(runID)` drops it. Neither is automatic; ask the owner which one the run's side
effects allow.
