package lint

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/ThallesP/durable"
)

type App struct{}

func (a *App) Register(e *durable.Engine, name string) {
	durable.Register(e, name, a.run) // want "workflow name must be a constant"
	durable.Register(e, "ok", a.run)
}

func (a *App) run(ctx durable.Context, nodes map[string]string) (int, error) {
	started := time.Now() // want "time.Now in a workflow"
	_ = started
	time.Sleep(time.Second) // want "time.Sleep in a workflow"
	_ = rand.IntN(3)        // want "math/rand/v2.IntN in a workflow"
	go func() {}()          // want "goroutine in a workflow"
	ch := make(chan int)
	select { // want "select in a workflow"
	case <-ch:
	default:
	}

	for id := range nodes { // want "map order changes"
		_ = durable.Do(ctx, "apply "+id, func(context.Context) error { return nil })
	}
	for id := range nodes {
		_ = id
	}

	var name string
	_ = durable.Do(ctx, name, func(context.Context) error { return nil })  // want "do name must start with a constant"
	_ = durable.Do(ctx, "a#b", func(context.Context) error { return nil }) // want "contains '#'"
	_ = durable.Do(ctx, fmt.Sprintf("node %s", name), func(context.Context) error { return nil })
	_, _ = durable.Step(ctx, "fine", func(context.Context) (time.Time, error) {
		_ = durable.Now(ctx) // want "durable.Now inside a step"
		return time.Now(), nil
	})
	durable.Sleep(ctx, "durable.x", time.Second) // want "reserved prefix"
	return 0, nil
}

func notAWorkflow() time.Time {
	return time.Now()
}
