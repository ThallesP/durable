package check

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"reflect"
	"regexp"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/types/typeutil"
)

type Manifest struct {
	Version   int                      `json:"version"`
	Workflows map[string]WorkflowShape `json:"workflows"`
}

type WorkflowShape struct {
	Input *Shape               `json:"input"`
	Steps map[string]StepShape `json:"steps"`
}

type StepShape struct {
	Kind string `json:"kind"`
	Type *Shape `json:"type,omitempty"`
}

type Problem struct {
	Pos     token.Position
	Message string
}

type funcBody struct {
	body *ast.BlockStmt
	info *types.Info
}

type builder struct {
	fset     *token.FileSet
	funcs    map[*types.Func]funcBody
	problems []Problem
}

func Build(pkgs []*packages.Package) (Manifest, []Problem) {
	b := &builder{funcs: map[*types.Func]funcBody{}}
	packages.Visit(pkgs, nil, func(pkg *packages.Package) {
		if b.fset == nil {
			b.fset = pkg.Fset
		}
		for _, file := range pkg.Syntax {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil || pkg.TypesInfo == nil {
					continue
				}
				if obj, ok := pkg.TypesInfo.Defs[fn.Name].(*types.Func); ok {
					b.funcs[obj] = funcBody{body: fn.Body, info: pkg.TypesInfo}
				}
			}
		}
	})

	manifest := Manifest{Version: 1, Workflows: map[string]WorkflowShape{}}
	for _, pkg := range pkgs {
		for _, file := range pkg.Syntax {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || durableFunc(pkg.TypesInfo, call) != "Register" || len(call.Args) < 3 {
					return true
				}
				b.register(manifest, pkg.TypesInfo, call)
				return true
			})
		}
	}
	return manifest, b.problems
}

func (b *builder) problem(pos token.Pos, format string, args ...any) {
	b.problems = append(b.problems, Problem{Pos: b.fset.Position(pos), Message: fmt.Sprintf(format, args...)})
}

func (b *builder) register(manifest Manifest, info *types.Info, call *ast.CallExpr) {
	name, ok := constString(info, call.Args[1])
	if !ok {
		return
	}
	typeArgs := instanceTypeArgs(info, call.Fun)
	if typeArgs == nil || typeArgs.Len() == 0 {
		b.problem(call.Pos(), "can't read the input type of workflow %q", name)
		return
	}

	workflow := WorkflowShape{Input: ShapeOf(typeArgs.At(0)), Steps: map[string]StepShape{}}
	body, bodyInfo := b.resolve(info, call.Args[2])
	if body == nil {
		b.problem(call.Args[2].Pos(), "can't find the body of workflow %q; pass a function or method declared in this module", name)
		return
	}
	b.collect(name, workflow.Steps, body, bodyInfo, map[*ast.BlockStmt]bool{})

	if existing, ok := manifest.Workflows[name]; ok {
		if !reflect.DeepEqual(existing, workflow) {
			b.problem(call.Pos(), "workflow %q is registered twice with different code", name)
		}
		return
	}
	manifest.Workflows[name] = workflow
}

func (b *builder) resolve(info *types.Info, expr ast.Expr) (*ast.BlockStmt, *types.Info) {
	if lit, ok := ast.Unparen(expr).(*ast.FuncLit); ok {
		return lit.Body, info
	}
	var obj types.Object
	switch e := ast.Unparen(expr).(type) {
	case *ast.Ident:
		obj = info.Uses[e]
	case *ast.SelectorExpr:
		if sel, ok := info.Selections[e]; ok {
			obj = sel.Obj()
		} else {
			obj = info.Uses[e.Sel]
		}
	}
	fn, ok := obj.(*types.Func)
	if !ok {
		return nil, nil
	}
	found, ok := b.funcs[fn.Origin()]
	if !ok {
		return nil, nil
	}
	return found.body, found.info
}

func (b *builder) collect(workflow string, steps map[string]StepShape, body *ast.BlockStmt, info *types.Info, visited map[*ast.BlockStmt]bool) {
	if visited[body] {
		return
	}
	visited[body] = true
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		op := durableFunc(info, call)
		if op == "" {
			b.follow(workflow, steps, info, call, visited)
			return true
		}
		kind := map[string]string{"Step": "step", "Do": "do", "Sleep": "sleep", "Await": "await"}[op]
		if kind == "" || len(call.Args) < 2 {
			return true
		}
		step := StepShape{Kind: kind}
		if kind == "step" || kind == "await" {
			if typeArgs := instanceTypeArgs(info, call.Fun); typeArgs != nil && typeArgs.Len() > 0 {
				step.Type = ShapeOf(typeArgs.At(0))
			}
		}
		name := namePattern(info, call.Args[1])
		if existing, ok := steps[name]; ok && !reflect.DeepEqual(existing, step) {
			b.problem(call.Pos(), "workflow %q uses %q twice as different kinds or types; replay would pair them up", workflow, name)
			return true
		}
		steps[name] = step
		return true
	})
}

func (b *builder) follow(workflow string, steps map[string]StepShape, info *types.Info, call *ast.CallExpr, visited map[*ast.BlockStmt]bool) {
	passesContext := false
	for _, arg := range call.Args {
		if isDurableContext(info.TypeOf(arg)) {
			passesContext = true
		}
	}
	if !passesContext {
		return
	}
	fn := typeutil.StaticCallee(info, call)
	if fn == nil {
		return
	}
	if found, ok := b.funcs[fn.Origin()]; ok {
		b.collect(workflow, steps, found.body, found.info, visited)
	}
}

func instanceTypeArgs(info *types.Info, fun ast.Expr) *types.TypeList {
	switch e := ast.Unparen(fun).(type) {
	case *ast.IndexExpr:
		return instanceTypeArgs(info, e.X)
	case *ast.IndexListExpr:
		return instanceTypeArgs(info, e.X)
	case *ast.SelectorExpr:
		return info.Instances[e.Sel].TypeArgs
	case *ast.Ident:
		return info.Instances[e].TypeArgs
	}
	return nil
}

var verb = regexp.MustCompile(`%[-+# 0-9.\[\]*]*[a-zA-Z%]`)

func namePattern(info *types.Info, expr ast.Expr) string {
	pattern := rawPattern(info, expr)
	for strings.Contains(pattern, "**") {
		pattern = strings.ReplaceAll(pattern, "**", "*")
	}
	return pattern
}

func rawPattern(info *types.Info, expr ast.Expr) string {
	if value, ok := constString(info, expr); ok {
		return value
	}
	switch e := ast.Unparen(expr).(type) {
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			return rawPattern(info, e.X) + rawPattern(info, e.Y)
		}
	case *ast.CallExpr:
		if format, ok := sprintfFormat(info, e); ok {
			return verb.ReplaceAllStringFunc(format, func(v string) string {
				if v == "%%" {
					return "%"
				}
				return "*"
			})
		}
	}
	return "*"
}
