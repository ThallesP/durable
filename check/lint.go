package check

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/types/typeutil"
)

const durablePath = "github.com/ThallesP/durable"

var Analyzer = &analysis.Analyzer{
	Name: "durable",
	Doc:  "reports workflow code that changes on replay or names steps in ways that break upgrades",
	Run: func(pass *analysis.Pass) (any, error) {
		for _, d := range Lint(pass.Files, pass.TypesInfo) {
			pass.Report(analysis.Diagnostic{Pos: d.Pos, Message: d.Message})
		}
		return nil, nil
	},
}

type Diagnostic struct {
	Pos     token.Pos
	Message string
}

type linter struct {
	info  *types.Info
	found []Diagnostic
	seen  map[token.Pos]bool
}

func Lint(files []*ast.File, info *types.Info) []Diagnostic {
	l := &linter{info: info, seen: map[token.Pos]bool{}}
	for _, file := range files {
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.FuncDecl:
				if n.Body != nil && takesDurableContext(l.info.TypeOf(n.Name)) {
					l.workflowBody(n.Body)
				}
			case *ast.FuncLit:
				if takesDurableContext(l.info.TypeOf(n)) {
					l.workflowBody(n.Body)
				}
			case *ast.CallExpr:
				if durableFunc(l.info, n) == "Register" && len(n.Args) > 1 {
					l.workflowName(n.Args[1])
				}
			}
			return true
		})
	}
	return l.found
}

func (l *linter) report(pos token.Pos, format string, args ...any) {
	if l.seen[pos] {
		return
	}
	l.seen[pos] = true
	l.found = append(l.found, Diagnostic{Pos: pos, Message: fmt.Sprintf(format, args...)})
}

func (l *linter) workflowBody(body *ast.BlockStmt) {
	ast.Inspect(body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			return !takesDurableContext(l.info.TypeOf(n))
		case *ast.GoStmt:
			l.report(n.Pos(), "goroutine in a workflow: replay can't follow it; start it inside durable.Step or use another workflow")
		case *ast.SelectStmt:
			l.report(n.Pos(), "select in a workflow doesn't survive a restart; use durable.Await or durable.Sleep")
			return false
		case *ast.SendStmt:
			l.report(n.Pos(), "channel send in a workflow doesn't survive a restart; use Engine.Signal and durable.Await")
		case *ast.UnaryExpr:
			if n.Op == token.ARROW {
				l.report(n.Pos(), "channel receive in a workflow doesn't survive a restart; use durable.Await")
			}
		case *ast.RangeStmt:
			l.rangeStmt(n)
		case *ast.CallExpr:
			return l.workflowCall(n)
		}
		return true
	})
}

func (l *linter) rangeStmt(n *ast.RangeStmt) {
	switch l.info.TypeOf(n.X).Underlying().(type) {
	case *types.Chan:
		l.report(n.Pos(), "ranging over a channel in a workflow doesn't survive a restart; use durable.Await")
	case *types.Map:
		if containsDurableCall(l.info, n.Body) {
			l.report(n.Pos(), "map order changes between runs, so replay pairs saved results with the wrong iteration; range over sorted keys (slices.Sorted(maps.Keys(m)))")
		}
	}
}

func (l *linter) workflowCall(call *ast.CallExpr) bool {
	switch op := durableFunc(l.info, call); op {
	case "Step", "Do":
		if len(call.Args) < 3 {
			return true
		}
		l.stepName(op, call.Args[1])
		ast.Inspect(call.Args[0], l.inspectWorkflow)
		if lit, ok := call.Args[2].(*ast.FuncLit); ok {
			l.stepBody(lit.Body)
		}
		for _, arg := range call.Args[3:] {
			ast.Inspect(arg, l.inspectWorkflow)
		}
		return false
	case "Sleep", "Await":
		if len(call.Args) > 1 {
			l.stepName(op, call.Args[1])
		}
		return true
	}

	fn, ok := typeutil.Callee(l.info, call).(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Signature().Recv() != nil {
		return true
	}
	pkg, name := fn.Pkg().Path(), fn.Name()
	switch {
	case pkg == "time" && (name == "Now" || name == "Since" || name == "Until"):
		l.report(call.Pos(), "time.%s in a workflow returns something else on replay; use durable.Now(ctx)", name)
	case pkg == "time" && (name == "Sleep" || name == "After" || name == "AfterFunc" || name == "Tick" || name == "NewTimer" || name == "NewTicker"):
		l.report(call.Pos(), "time.%s in a workflow doesn't survive a restart; use durable.Sleep or durable.Await", name)
	case pkg == "math/rand" || pkg == "math/rand/v2" || pkg == "crypto/rand":
		l.report(call.Pos(), "%s.%s in a workflow returns something else on replay; call it inside durable.Step", pkg, name)
	}
	return true
}

func (l *linter) inspectWorkflow(n ast.Node) bool {
	if call, ok := n.(*ast.CallExpr); ok {
		return l.workflowCall(call)
	}
	return true
}

func (l *linter) stepBody(body *ast.BlockStmt) {
	ast.Inspect(body, func(n ast.Node) bool {
		if lit, ok := n.(*ast.FuncLit); ok {
			return !takesDurableContext(l.info.TypeOf(lit))
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if op := durableFunc(l.info, call); op != "" && op != "Register" {
			l.report(call.Pos(), "durable.%s inside a step: steps can't run durable calls; move it to the workflow body", op)
		}
		return true
	})
}

func (l *linter) workflowName(expr ast.Expr) {
	value, ok := constString(l.info, expr)
	if !ok {
		l.report(expr.Pos(), "workflow name must be a constant string: runs in flight find their code by it")
		return
	}
	if value == "" {
		l.report(expr.Pos(), "workflow name is empty")
	}
}

func (l *linter) stepName(op string, expr ast.Expr) {
	prefix, constant := namePrefix(l.info, expr)
	switch {
	case constant && prefix == "":
		l.report(expr.Pos(), "%s name is empty", strings.ToLower(op))
	case prefix == "":
		l.report(expr.Pos(), "%s name must start with a constant (\"apply \" + svc.Name) so saved results keep matching it after an upgrade", strings.ToLower(op))
	case strings.Contains(prefix, "#"):
		l.report(expr.Pos(), "%s name contains '#', which durable uses to count repeated names", strings.ToLower(op))
	case strings.HasPrefix(prefix, "durable."):
		l.report(expr.Pos(), "%s name uses the reserved prefix \"durable.\"", strings.ToLower(op))
	}
}

func namePrefix(info *types.Info, expr ast.Expr) (string, bool) {
	if value, ok := constString(info, expr); ok {
		return value, true
	}
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return namePrefix(info, e.X)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		prefix, _ := namePrefix(info, e.X)
		return prefix, false
	case *ast.CallExpr:
		format, ok := sprintfFormat(info, e)
		if !ok {
			return "", false
		}
		prefix, _, _ := strings.Cut(format, "%")
		return prefix, false
	}
	return "", false
}

func sprintfFormat(info *types.Info, call *ast.CallExpr) (string, bool) {
	fn, ok := typeutil.Callee(info, call).(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "fmt" || fn.Name() != "Sprintf" || len(call.Args) == 0 {
		return "", false
	}
	return constString(info, call.Args[0])
}

func constString(info *types.Info, expr ast.Expr) (string, bool) {
	tv, ok := info.Types[expr]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(tv.Value), true
}

func durableFunc(info *types.Info, call *ast.CallExpr) string {
	fn, ok := typeutil.Callee(info, call).(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != durablePath || fn.Signature().Recv() != nil {
		return ""
	}
	return fn.Name()
}

func containsDurableCall(info *types.Info, node ast.Node) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && durableFunc(info, call) != "" {
			found = true
		}
		return !found
	})
	return found
}

func isDurableContext(t types.Type) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == durablePath && obj.Name() == "Context"
}

func takesDurableContext(t types.Type) bool {
	sig, ok := t.(*types.Signature)
	if !ok {
		return false
	}
	for param := range sig.Params().Variables() {
		if isDurableContext(param.Type()) {
			return true
		}
	}
	return false
}
