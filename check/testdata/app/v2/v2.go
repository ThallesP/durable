package v2

import (
	"context"
	"time"

	"github.com/ThallesP/durable"
)

type Input struct {
	DeploymentID string
	Env          string
	Force        bool
}

type Plan struct {
	Nodes    []string `json:"nodeIDs"`
	Replicas float64
	At       string
	Region   string
}

type App struct{}

func (a *App) Register(e *durable.Engine) {
	durable.Register(e, "deploy", a.deploy)
}

func (a *App) deploy(ctx durable.Context, in Input) (int, error) {
	if _, err := durable.Step(ctx, "audit", func(context.Context) (string, error) { return "", nil }); err != nil {
		return 0, err
	}
	plan, err := durable.Step(ctx, "plan", func(context.Context) (Plan, error) { return Plan{}, nil })
	if err != nil {
		return 0, err
	}
	for _, id := range plan.Nodes {
		if _, err := durable.Step(ctx, "apply "+id, func(context.Context) (int, error) { return 0, nil }); err != nil {
			return 0, err
		}
	}
	if err := durable.Do(ctx, "settle", func(context.Context) error { return nil }); err != nil {
		return 0, err
	}
	_, err = durable.Await[bool](ctx, "converged", time.Hour)
	return len(plan.Nodes), err
}
