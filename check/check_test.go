package check_test

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/ThallesP/durable/check"
	"golang.org/x/tools/go/packages"
)

func load(t *testing.T, pattern string) []*packages.Package {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Dir:  filepath.Join("testdata", "app"),
	}
	pkgs, err := packages.Load(cfg, pattern)
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("testdata doesn't compile")
	}
	return pkgs
}

var want = regexp.MustCompile(`// want "([^"]+)"`)

func TestLint(t *testing.T) {
	pkg := load(t, "./lint")[0]
	got := map[int][]string{}
	for _, d := range check.Lint(pkg.Syntax, pkg.TypesInfo) {
		line := pkg.Fset.Position(d.Pos).Line
		got[line] = append(got[line], d.Message)
	}

	file, err := os.Open(pkg.GoFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		match := want.FindStringSubmatch(scanner.Text())
		if match == nil {
			if len(got[line]) > 0 {
				t.Errorf("line %d: unexpected %q", line, got[line])
			}
			continue
		}
		if !slices.ContainsFunc(got[line], func(msg string) bool { return strings.Contains(msg, match[1]) }) {
			t.Errorf("line %d: want %q, got %q", line, match[1], got[line])
		}
	}
}

func manifest(t *testing.T, pattern string) check.Manifest {
	t.Helper()
	m, problems := check.Build(load(t, pattern))
	if len(problems) > 0 {
		t.Fatalf("problems: %v", problems)
	}
	return m
}

func TestManifest(t *testing.T) {
	m := manifest(t, "./v1")
	deploy, ok := m.Workflows["deploy"]
	if !ok {
		t.Fatalf("workflows = %v", m.Workflows)
	}
	steps := []string{}
	for name, step := range deploy.Steps {
		steps = append(steps, name+":"+step.Kind)
	}
	slices.Sort(steps)
	if got := strings.Join(steps, ","); got != "apply *:do,converged:await,plan:step,settle:sleep" {
		t.Fatalf("steps = %s", got)
	}
	plan := deploy.Steps["plan"].Type
	if plan.Fields["At"].Kind != "custom" || plan.Fields["NodeIDs"].String() != "[]string" {
		t.Fatalf("plan shape = %+v", plan.Fields)
	}
	if _, ok := m.Workflows["cleanup"].Steps["prune"]; !ok {
		t.Fatalf("cleanup steps = %v", m.Workflows["cleanup"].Steps)
	}
}

func TestCompare(t *testing.T) {
	changes := check.Compare(manifest(t, "./v1"), manifest(t, "./v2"))
	got := []string{}
	for _, c := range changes {
		got = append(got, c.String())
	}
	wants := []struct {
		breaking bool
		text     string
	}{
		{false, `workflow "cleanup": no longer registered`},
		{false, `workflow "deploy": input changed compatibly`},
		{false, `workflow "deploy" step "audit": new`},
		{true, `workflow "deploy" step "plan": At was time.Time, now string`},
		{true, `workflow "deploy" step "apply *": do became step`},
		{true, `workflow "deploy" step "settle": was a sleep, now a do`},
	}
	for _, w := range wants {
		prefix := ""
		if w.breaking {
			prefix = "BREAKING "
		}
		if !slices.ContainsFunc(got, func(line string) bool { return strings.HasPrefix(line, prefix+w.text) }) {
			t.Errorf("missing %q in:\n%s", prefix+w.text, strings.Join(got, "\n"))
		}
	}
	if len(got) != len(wants) {
		t.Errorf("got %d changes, want %d:\n%s", len(got), len(wants), strings.Join(got, "\n"))
	}
}

func TestCompatibleShapes(t *testing.T) {
	str := &check.Shape{Kind: "string"}
	num := func(kind string) *check.Shape { return &check.Shape{Kind: kind} }
	cases := []struct {
		name         string
		saved, now   *check.Shape
		isCompatible bool
	}{
		{"same", str, str, true},
		{"int to float", num("int"), num("float"), true},
		{"float to int", num("float"), num("int"), false},
		{"anything to any", str, num("any"), true},
		{"field added", &check.Shape{Kind: "struct", Fields: map[string]*check.Shape{"a": str}}, &check.Shape{Kind: "struct", Fields: map[string]*check.Shape{"a": str, "b": str}}, true},
		{"field removed", &check.Shape{Kind: "struct", Fields: map[string]*check.Shape{"a": str, "b": str}}, &check.Shape{Kind: "struct", Fields: map[string]*check.Shape{"a": str}}, false},
		{"field case", &check.Shape{Kind: "struct", Fields: map[string]*check.Shape{"Name": str}}, &check.Shape{Kind: "struct", Fields: map[string]*check.Shape{"name": str}}, true},
		{"list elem", &check.Shape{Kind: "list", Elem: str}, &check.Shape{Kind: "list", Elem: num("int")}, false},
	}
	for _, c := range cases {
		if got := len(check.Incompatible(c.saved, c.now)) == 0; got != c.isCompatible {
			t.Errorf("%s: compatible = %v", c.name, got)
		}
	}
}
