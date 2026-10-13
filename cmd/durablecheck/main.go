package main

import (
	"flag"
	"fmt"
	"os"
	"slices"

	"github.com/ThallesP/durable/check"
	"golang.org/x/tools/go/packages"
)

const usage = `durablecheck checks durable workflows before they ship.

  durablecheck [flags] [packages]     lint workflows and compare them with the lock file
  durablecheck -update [packages]     record the current workflows in the lock file

Packages default to ./... Flags:
`

func main() {
	update := flag.Bool("update", false, "write the lock file (refuses breaking changes unless -force)")
	force := flag.Bool("force", false, "with -update, record breaking changes too; only when no run of the affected workflows is in flight")
	lockPath := flag.String("lock", "durable.lock", "lock file path")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()
	patterns := flag.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	os.Exit(run(patterns, *lockPath, *update, *force))
}

func run(patterns []string, lockPath string, update, force bool) int {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if packages.PrintErrors(pkgs) > 0 {
		return 2
	}

	failed := false
	for _, pkg := range pkgs {
		for _, d := range check.Lint(pkg.Syntax, pkg.TypesInfo) {
			fmt.Printf("%s: %s\n", pkg.Fset.Position(d.Pos), d.Message)
			failed = true
		}
	}
	current, problems := check.Build(pkgs)
	for _, p := range problems {
		fmt.Printf("%s: %s\n", p.Pos, p.Message)
		failed = true
	}

	locked, exists, err := check.ReadLock(lockPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	changes := check.Compare(locked, current)
	breaking := slices.ContainsFunc(changes, func(c check.Change) bool { return c.Breaking })
	for _, change := range changes {
		fmt.Println(change)
	}

	if !update {
		if len(changes) > 0 || !exists && len(current.Workflows) > 0 {
			failed = true
			fmt.Printf("\n%s is out of date.\n", lockPath)
			if breaking {
				fmt.Println(breakingHelp)
				return 1
			}
			fmt.Println("Record these changes with: durablecheck -update")
		}
		return exitCode(failed)
	}

	if breaking && !force {
		fmt.Printf("\nNot writing %s.\n%s\n", lockPath, breakingHelp)
		return 1
	}
	if err := check.WriteLock(lockPath, current); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	fmt.Printf("wrote %s (%d workflows)\n", lockPath, len(current.Workflows))
	return exitCode(failed)
}

const breakingHelp = `Breaking changes would stall or corrupt runs that are in flight when this ships. Pick one:
  - keep the old shape: add a field instead of renaming or retyping one
  - give the step a new name: runs in flight run it again (it must be idempotent)
  - register the new code under a new workflow name and keep the old one until its runs finish
  - -update -force, only when no run of that workflow can be in flight anywhere this code runs
See the durable-workflows skill.`

func exitCode(failed bool) int {
	if failed {
		return 1
	}
	return 0
}
