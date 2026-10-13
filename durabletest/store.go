package durabletest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThallesP/durable"
)

type Open func(t *testing.T) durable.Store

func Store(t *testing.T, open Open) {
	cases := []struct {
		name string
		run  func(t *testing.T, s durable.Store)
	}{
		{"CreateRunIsIdempotent", createRunIsIdempotent},
		{"GetRunNotFound", getRunNotFound},
		{"ListRunsNewestFirst", listRunsNewestFirst},
		{"ClaimTakesPendingInOrder", claimTakesPendingInOrder},
		{"ClaimRespectsConcurrency", claimRespectsConcurrency},
		{"ClaimTakesDueAndAbandoned", claimTakesDueAndAbandoned},
		{"WritesNeedTheLease", writesNeedTheLease},
		{"StepsKeepOrderOnUpdate", stepsKeepOrderOnUpdate},
		{"SignalWakesWaitingRun", signalWakesWaitingRun},
		{"ReleaseSeesPendingSignal", releaseSeesPendingSignal},
		{"ConsumeSignalIsFIFO", consumeSignalIsFIFO},
		{"FinishRun", finishRun},
		{"RenewLeases", renewLeases},
		{"Cancel", cancel},
		{"Restart", restart},
		{"Unstall", unstall},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.run(t, open(t)) })
	}
}

var ctx = context.Background()

const owner = "owner-a"

func newRun(id string) durable.Run {
	now := time.Now()
	return durable.Run{ID: id, Workflow: "wf", Input: []byte(`{}`), Status: durable.StatusPending, CreatedAt: now, UpdatedAt: now}
}

func create(t *testing.T, s durable.Store, runs ...durable.Run) {
	t.Helper()
	for _, run := range runs {
		if _, _, err := s.CreateRun(ctx, run); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

func claim(t *testing.T, s durable.Store, who string, limit int) []durable.Run {
	t.Helper()
	now := time.Now()
	runs, err := s.ClaimRuns(ctx, durable.Claim{Owner: who, Limit: limit, Now: now, Until: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	return runs
}

func ids(runs []durable.Run) []string {
	out := []string{}
	for _, run := range runs {
		out = append(out, run.ID)
	}
	return out
}

func get(t *testing.T, s durable.Store, id string) durable.Run {
	t.Helper()
	run, err := s.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func equal[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func equalIDs(t *testing.T, what string, got []durable.Run, want ...string) {
	t.Helper()
	gotIDs := ids(got)
	if len(gotIDs) != len(want) {
		t.Fatalf("%s = %v, want %v", what, gotIDs, want)
	}
	for i := range want {
		if gotIDs[i] != want[i] {
			t.Fatalf("%s = %v, want %v", what, gotIDs, want)
		}
	}
}

func mustErr(t *testing.T, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: err = %v, want %v", what, err, want)
	}
}

func createRunIsIdempotent(t *testing.T, s durable.Store) {
	run, created, err := s.CreateRun(ctx, newRun("a"))
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "created", created, true)
	equal(t, "status", run.Status, durable.StatusPending)

	other := newRun("a")
	other.Workflow = "other"
	again, created, err := s.CreateRun(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "created again", created, false)
	equal(t, "workflow", again.Workflow, "wf")
}

func getRunNotFound(t *testing.T, s durable.Store) {
	_, err := s.GetRun(ctx, "missing")
	mustErr(t, "get", err, durable.ErrNotFound)
}

func listRunsNewestFirst(t *testing.T, s durable.Store) {
	other := newRun("c")
	other.Workflow = "other"
	create(t, s, newRun("a"), newRun("b"), other)

	all, err := s.ListRuns(ctx, durable.RunFilter{})
	if err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "all", all, "c", "b", "a")

	wf, err := s.ListRuns(ctx, durable.RunFilter{Workflow: "wf", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "wf limit 1", wf, "b")

	claim(t, s, owner, 1)
	running, err := s.ListRuns(ctx, durable.RunFilter{Statuses: []durable.Status{durable.StatusRunning}})
	if err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "running", running, "a")
}

func claimTakesPendingInOrder(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"), newRun("b"), newRun("c"))

	first := claim(t, s, owner, 2)
	equalIDs(t, "first claim", first, "a", "b")
	equal(t, "status", first[0].Status, durable.StatusRunning)
	equal(t, "owner", first[0].Owner, owner)

	run := get(t, s, "a")
	equal(t, "stored status", run.Status, durable.StatusRunning)
	equal(t, "stored owner", run.Owner, owner)
	equal(t, "leased", run.LeaseUntil.After(time.Now()), true)

	equalIDs(t, "second claim", claim(t, s, "owner-b", 5), "c")
	equalIDs(t, "third claim", claim(t, s, "owner-b", 5))
}

func claimRespectsConcurrency(t *testing.T, s durable.Store) {
	limited := func(id, key string) durable.Run {
		run := newRun(id)
		run.ConcurrencyKey = key
		run.ConcurrencyLimit = 1
		return run
	}
	create(t, s, limited("a", "env-1"), limited("b", "env-1"), limited("c", "env-2"))

	equalIDs(t, "first claim", claim(t, s, owner, 10), "a", "c")
	if err := s.Release(ctx, owner, "a", durable.Wake{At: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "while a waits", claim(t, s, owner, 10))

	if err := s.Release(ctx, owner, "c", durable.Wake{At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "c is due", claim(t, s, owner, 10), "c")
	if err := s.FinishRun(ctx, owner, "c", durable.Finish{Status: durable.StatusSucceeded}); err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "env-1 still held", claim(t, s, owner, 10))
}

func claimTakesDueAndAbandoned(t *testing.T, s durable.Store) {
	create(t, s, newRun("due"), newRun("later"), newRun("signal-only"), newRun("abandoned"))
	claim(t, s, owner, 3)
	now := time.Now()
	for id, wake := range map[string]durable.Wake{
		"due":         {At: now.Add(-time.Second)},
		"later":       {At: now.Add(time.Hour)},
		"signal-only": {Signal: "go"},
	} {
		if err := s.Release(ctx, owner, id, wake); err != nil {
			t.Fatal(err)
		}
	}
	expired := time.Now()
	if _, err := s.ClaimRuns(ctx, durable.Claim{Owner: "crashed", Limit: 1, Now: expired, Until: expired.Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}

	equalIDs(t, "claim", claim(t, s, "owner-b", 10), "due", "abandoned")
	equal(t, "new owner", get(t, s, "abandoned").Owner, "owner-b")
}

func writesNeedTheLease(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"))
	step := durable.StepRecord{Key: "x", Name: "x", Kind: durable.KindStep, Status: durable.StepDone, StartedAt: time.Now()}

	mustErr(t, "save before claim", s.SaveStep(ctx, owner, "a", step), durable.ErrLeaseLost)
	claim(t, s, owner, 1)
	mustErr(t, "save by another owner", s.SaveStep(ctx, "owner-b", "a", step), durable.ErrLeaseLost)
	mustErr(t, "release by another owner", s.Release(ctx, "owner-b", "a", durable.Wake{}), durable.ErrLeaseLost)
	mustErr(t, "finish by another owner", s.FinishRun(ctx, "owner-b", "a", durable.Finish{Status: durable.StatusSucceeded}), durable.ErrLeaseLost)
	_, _, err := s.ConsumeSignal(ctx, "owner-b", "a", "go", step)
	mustErr(t, "consume by another owner", err, durable.ErrLeaseLost)
	if err := s.SaveStep(ctx, owner, "a", step); err != nil {
		t.Fatal(err)
	}
}

func stepsKeepOrderOnUpdate(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"))
	claim(t, s, owner, 1)
	started := time.Now().Truncate(time.Millisecond)
	for _, key := range []string{"first", "second", "third"} {
		step := durable.StepRecord{Key: key, Name: key, Kind: durable.KindSleep, Status: durable.StepPending, Deadline: started.Add(time.Hour), StartedAt: started}
		if err := s.SaveStep(ctx, owner, "a", step); err != nil {
			t.Fatal(err)
		}
	}
	done := durable.StepRecord{
		Key: "first", Name: "first", Kind: durable.KindStep, Status: durable.StepDone, Output: []byte(`{"n":1}`),
		Error: "boom", TimedOut: true, Attempts: 3, StartedAt: started, FinishedAt: started.Add(time.Second),
	}
	if err := s.SaveStep(ctx, owner, "a", done); err != nil {
		t.Fatal(err)
	}

	steps, err := s.Steps(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "steps", len(steps), 3)
	equal(t, "order", steps[0].Key+","+steps[1].Key+","+steps[2].Key, "first,second,third")
	got := steps[0]
	equal(t, "kind", got.Kind, durable.KindStep)
	equal(t, "status", got.Status, durable.StepDone)
	equal(t, "output", string(got.Output), `{"n":1}`)
	equal(t, "error", got.Error, "boom")
	equal(t, "timed out", got.TimedOut, true)
	equal(t, "attempts", got.Attempts, 3)
	equal(t, "started", got.StartedAt.Equal(started), true)
	equal(t, "finished", got.FinishedAt.Equal(started.Add(time.Second)), true)
	equal(t, "deadline", steps[1].Deadline.Equal(started.Add(time.Hour)), true)
}

func signalWakesWaitingRun(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"))
	mustErr(t, "signal missing run", s.SendSignal(ctx, "missing", "go", nil), durable.ErrNotFound)
	claim(t, s, owner, 1)
	if err := s.Release(ctx, owner, "a", durable.Wake{Signal: "go"}); err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "before signal", claim(t, s, owner, 1))

	if err := s.SendSignal(ctx, "a", "other", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "after another signal", claim(t, s, owner, 1))

	if err := s.SendSignal(ctx, "a", "go", []byte(`2`)); err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "after signal", claim(t, s, owner, 1), "a")
}

func releaseSeesPendingSignal(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"))
	claim(t, s, owner, 1)
	if err := s.SendSignal(ctx, "a", "go", []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, owner, "a", durable.Wake{At: time.Now().Add(time.Hour), Signal: "go"}); err != nil {
		t.Fatal(err)
	}
	equalIDs(t, "claim", claim(t, s, owner, 1), "a")
}

func consumeSignalIsFIFO(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"))
	claim(t, s, owner, 1)
	for _, payload := range []string{`1`, `2`} {
		if err := s.SendSignal(ctx, "a", "go", []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	consume := func(key string) (durable.StepRecord, bool) {
		step := durable.StepRecord{Key: key, Name: "go", Kind: durable.KindAwait, Status: durable.StepDone, StartedAt: time.Now()}
		got, ok, err := s.ConsumeSignal(ctx, owner, "a", "go", step)
		if err != nil {
			t.Fatal(err)
		}
		return got, ok
	}

	first, ok := consume("go")
	equal(t, "first ok", ok, true)
	equal(t, "first payload", string(first.Output), `1`)
	second, _ := consume("go#2")
	equal(t, "second payload", string(second.Output), `2`)
	_, ok = consume("go#3")
	equal(t, "third ok", ok, false)

	steps, err := s.Steps(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "saved steps", len(steps), 2)
	equal(t, "saved payload", string(steps[1].Output), `2`)
}

func finishRun(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"))
	claim(t, s, owner, 1)
	if err := s.FinishRun(ctx, owner, "a", durable.Finish{Status: durable.StatusSucceeded, Output: []byte(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	run := get(t, s, "a")
	equal(t, "status", run.Status, durable.StatusSucceeded)
	equal(t, "output", string(run.Output), `{"ok":true}`)
	equal(t, "owner", run.Owner, "")
	equal(t, "finished", run.FinishedAt.IsZero(), false)
	equalIDs(t, "claim", claim(t, s, owner, 1))
	if err := s.SendSignal(ctx, "a", "go", nil); err != nil {
		t.Fatal(err)
	}
}

func renewLeases(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"), newRun("b"), newRun("c"))
	claim(t, s, owner, 2)
	if _, err := s.Cancel(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Hour)
	leases, err := s.RenewLeases(ctx, owner, []string{"a", "b", "c"}, until)
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "cancelled", len(leases.Cancelled) == 1 && leases.Cancelled[0] == "b", true)
	equal(t, "lost", len(leases.Lost) == 1 && leases.Lost[0] == "c", true)
	equal(t, "renewed", get(t, s, "a").LeaseUntil.Sub(until).Abs() < time.Millisecond, true)
}

func cancel(t *testing.T, s durable.Store) {
	create(t, s, newRun("pending"), newRun("running"))
	_, err := s.Cancel(ctx, "missing")
	mustErr(t, "cancel missing", err, durable.ErrNotFound)

	run, err := s.Cancel(ctx, "pending")
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "pending status", run.Status, durable.StatusCancelled)
	equal(t, "pending finished", get(t, s, "pending").FinishedAt.IsZero(), false)

	claim(t, s, owner, 1)
	run, err = s.Cancel(ctx, "running")
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "running status", run.Status, durable.StatusRunning)
	equal(t, "cancel requested", get(t, s, "running").CancelRequested, true)
}

func restart(t *testing.T, s durable.Store) {
	create(t, s, newRun("a"))
	mustErr(t, "restart pending", s.Restart(ctx, "a"), durable.ErrRunActive)
	claim(t, s, owner, 1)
	step := durable.StepRecord{Key: "x", Name: "x", Kind: durable.KindStep, Status: durable.StepDone, StartedAt: time.Now()}
	if err := s.SaveStep(ctx, owner, "a", step); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishRun(ctx, owner, "a", durable.Finish{Status: durable.StatusFailed, Error: "boom"}); err != nil {
		t.Fatal(err)
	}

	if err := s.Restart(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	run := get(t, s, "a")
	equal(t, "status", run.Status, durable.StatusPending)
	equal(t, "error", run.Error, "")
	steps, err := s.Steps(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "steps", len(steps), 0)
	equalIDs(t, "claim", claim(t, s, owner, 1), "a")
}

func unstall(t *testing.T, s durable.Store) {
	other := newRun("b")
	other.Workflow = "other"
	create(t, s, newRun("a"), other)
	claim(t, s, owner, 2)
	for _, id := range []string{"a", "b"} {
		if err := s.FinishRun(ctx, owner, id, durable.Finish{Status: durable.StatusStalled, Error: "not registered"}); err != nil {
			t.Fatal(err)
		}
	}
	equal(t, "stalled is not finished", get(t, s, "a").FinishedAt.IsZero(), true)

	count, err := s.Unstall(ctx, []string{"wf"})
	if err != nil {
		t.Fatal(err)
	}
	equal(t, "count", count, 1)
	equal(t, "a status", get(t, s, "a").Status, durable.StatusWaiting)
	equal(t, "b status", get(t, s, "b").Status, durable.StatusStalled)
	equalIDs(t, "claim", claim(t, s, owner, 5), "a")
}
