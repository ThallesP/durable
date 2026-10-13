package check

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

type Change struct {
	Workflow string
	Step     string
	Breaking bool
	Message  string
}

func (c Change) String() string {
	where := fmt.Sprintf("workflow %q", c.Workflow)
	if c.Step != "" {
		where += fmt.Sprintf(" step %q", c.Step)
	}
	if c.Breaking {
		return "BREAKING " + where + ": " + c.Message
	}
	return where + ": " + c.Message
}

func Compare(locked, current Manifest) []Change {
	changes := []Change{}
	for _, name := range sortedKeys(current.Workflows) {
		if _, ok := locked.Workflows[name]; !ok {
			changes = append(changes, Change{Workflow: name, Message: "new workflow"})
		}
	}
	for _, name := range sortedKeys(locked.Workflows) {
		was := locked.Workflows[name]
		now, ok := current.Workflows[name]
		if !ok {
			changes = append(changes, Change{Workflow: name, Message: "no longer registered; runs of it still in flight stall until it is, so remove it only after they finish"})
			continue
		}
		problems := Incompatible(was.Input, now.Input)
		for _, problem := range problems {
			changes = append(changes, Change{Workflow: name, Breaking: true, Message: "input: " + problem})
		}
		if len(problems) == 0 && !sameShape(was.Input, now.Input) {
			changes = append(changes, Change{Workflow: name, Message: "input changed compatibly: " + Describe(was.Input, now.Input)})
		}
		changes = append(changes, compareSteps(name, was.Steps, now.Steps)...)
	}
	return changes
}

func compareSteps(workflow string, locked, current map[string]StepShape) []Change {
	changes := []Change{}
	for _, name := range sortedKeys(current) {
		if _, ok := locked[name]; !ok {
			changes = append(changes, Change{Workflow: workflow, Step: name, Message: "new; runs in flight run it when they reach it, so it can't rely on a step after it (a renamed step runs again)"})
		}
	}
	for _, name := range sortedKeys(locked) {
		was := locked[name]
		now, ok := current[name]
		if !ok {
			changes = append(changes, Change{Workflow: workflow, Step: name, Message: "removed; runs in flight ignore its saved result"})
			continue
		}
		if was.Kind != now.Kind {
			if was.Kind == "step" && now.Kind == "do" {
				changes = append(changes, Change{Workflow: workflow, Step: name, Message: "step became do; its saved result is ignored"})
				continue
			}
			if was.Kind == "do" && now.Kind == "step" {
				changes = append(changes, Change{Workflow: workflow, Step: name, Breaking: true, Message: "do became step; runs in flight that already ran it get a zero value"})
				continue
			}
			changes = append(changes, Change{Workflow: workflow, Step: name, Breaking: true, Message: fmt.Sprintf("was a %s, now a %s; runs in flight stall on it", was.Kind, now.Kind)})
			continue
		}
		problems := Incompatible(was.Type, now.Type)
		for _, problem := range problems {
			changes = append(changes, Change{Workflow: workflow, Step: name, Breaking: true, Message: problem})
		}
		if len(problems) == 0 && !sameShape(was.Type, now.Type) {
			changes = append(changes, Change{Workflow: workflow, Step: name, Message: "type changed compatibly: " + Describe(was.Type, now.Type)})
		}
	}
	return changes
}

func sameShape(a, b *Shape) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func ReadLock(path string) (Manifest, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Manifest{Version: 1, Workflows: map[string]WorkflowShape{}}, false, nil
	}
	if err != nil {
		return Manifest{}, false, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, false, fmt.Errorf("%s: %w", path, err)
	}
	return manifest, true, nil
}

func WriteLock(path string, manifest Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
