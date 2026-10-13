package v1

import (
	"context"
	"time"

	"github.com/ThallesP/durable"
)

type Input struct {
	DeploymentID string
	Env          string
}

type Plan struct {
	NodeIDs  []string
	Replicas int
	At       time.Time
}

type App struct{}

func (a *App) Register(e *durable.Engine) {
	durable.Register(e, "deploy", a.deploy)
	durable.Register(e, "cleanup", func(ctx durable.Context, in string) (bool, error) {
		return true, durable.Do(ctx, "prune", func(context.Context) error { return nil })
	})
}

func (a *App) deploy(ctx durable.Context, in Input) (int, error) {
	plan, err := durable.Step(ctx, "plan", func(context.Context) (Plan, error) { return Plan{}, nil })
	if err != nil {
		return 0, err
	}
	if err := a.apply(ctx, plan); err != nil {
		return 0, err
	}
	durable.Sleep(ctx, "settle", time.Minute)
	_, err = durable.Await[bool](ctx, "converged", time.Hour)
	return len(plan.NodeIDs), err
}

func (a *App) apply(ctx durable.Context, plan Plan) error {
	for _, id := range plan.NodeIDs {
		if err := durable.Do(ctx, "apply "+id, func(context.Context) error { return nil }); err != nil {
			return err
		}
	}
	return nil
}
