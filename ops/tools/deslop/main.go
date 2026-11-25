// Command deslop is an MDS-only architectural linter for dst, controlplane,
// and broker: module-layer, tangle, shallow-package, field-leak.
package main

import (
	"fmt"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

const (
	mdsPrefix  = "github.com/manifesterio/mds/"
	heldInMind = 7
	breakEven  = 1
	toolName   = "deslop"
)

var modules = []string{
	"github.com/manifesterio/mds/dst",
	"github.com/manifesterio/mds/controlplane",
	"github.com/manifesterio/mds/broker",
}

func main() {
	root, err := repoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", toolName, err)
		os.Exit(2)
	}
	fmt.Fprintf(os.Stderr, "%s: loading dst, controlplane, broker...\n", toolName)
	pkgs, err := loadModules(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", toolName, err)
		os.Exit(2)
	}

	var diags []diagnostic
	diags = append(diags, checkModuleLayer(root)...)
	diags = append(diags, checkTangle(pkgs)...)
	diags = append(diags, checkShallow(pkgs)...)
	diags = append(diags, checkFieldLeak(pkgs)...)
	sort.Slice(diags, func(i, j int) bool {
		if diags[i].lint != diags[j].lint {
			return diags[i].lint < diags[j].lint
		}
		return diags[i].key < diags[j].key
	})

	for _, d := range diags {
		fmt.Print(d.Format())
	}
	printSummary(diags)
	if len(diags) == 0 {
		fmt.Printf("%s: no issues in dst/controlplane/broker\n", toolName)
		return
	}
	fmt.Printf("\n%s: %d errors in dst/controlplane/broker\n", toolName, len(diags))
	os.Exit(1)
}

func printSummary(diags []diagnostic) {
	byLint := map[string]int{}
	byArea := map[string]int{}
	for _, d := range diags {
		byLint[d.lint]++
		byArea[diagnosticArea(d)]++
	}
	fmt.Fprintf(os.Stderr, "\n%s summary:\n", toolName)
	for _, lint := range []string{"module-layer", "tangle", "shallow-package", "field-leak"} {
		fmt.Fprintf(os.Stderr, "  %-16s %d\n", lint, byLint[lint])
	}
	fmt.Fprintf(os.Stderr, "  by area: dst=%d controlplane=%d broker=%d repo=%d other=%d\n",
		byArea["dst"], byArea["controlplane"], byArea["broker"], byArea["repo"], byArea["other"])
}

func diagnosticArea(d diagnostic) string {
	if d.lint == "module-layer" {
		return "repo"
	}
	for _, cand := range []string{d.file, d.key, d.message} {
		switch {
		case strings.Contains(cand, "dst/") || strings.HasPrefix(cand, "dst") || strings.HasPrefix(cand, "`dst"):
			return "dst"
		case strings.Contains(cand, "controlplane"):
			return "controlplane"
		case strings.Contains(cand, "broker"):
			return "broker"
		}
	}
	return "other"
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "dst", "go.mod")); err == nil {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("not inside the MDS repo (no go.work + dst/)")
		}
		dir = parent
	}
}

func loadModules(root string) ([]*packages.Package, error) {
	patterns := make([]string, len(modules))
	for i, m := range modules {
		patterns[i] = m + "/..."
	}
	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedFiles |
			packages.NeedCompiledGoFiles |
			packages.NeedImports |
			packages.NeedSyntax |
			packages.NeedTypes |
			packages.NeedTypesInfo |
			packages.NeedTypesSizes |
			packages.NeedModule,
		Dir: root,
		Env: append(os.Environ(), "GOWORK="+filepath.Join(root, "go.work")),
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, err
	}
	var out []*packages.Package
	var loadErrs strings.Builder
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		if !inModules(p.PkgPath) {
			return
		}
		if skipPath(p.PkgPath) {
			return
		}
		for _, e := range p.Errors {
			fmt.Fprintf(&loadErrs, "\n  %s: %s", p.PkgPath, e)
		}
		if len(p.Errors) > 0 || p.Types == nil || p.TypesInfo == nil {
			return
		}
		out = append(out, p)
	})
	if len(out) == 0 {
		if loadErrs.Len() > 0 {
			return nil, fmt.Errorf("failed to load packages:%s", loadErrs.String())
		}
		return nil, fmt.Errorf("no packages loaded")
	}
	if loadErrs.Len() > 0 {
		fmt.Fprintf(os.Stderr, "%s: warning: some packages failed to load:%s\n", toolName, loadErrs.String())
	}
	return out, nil
}

func inModules(pkgPath string) bool {
	for _, m := range modules {
		if pkgPath == m || strings.HasPrefix(pkgPath, m+"/") {
			return true
		}
	}
	return false
}

func inMDS(pkgPath string) bool {
	return strings.HasPrefix(pkgPath, mdsPrefix)
}

func skipPath(pkgPath string) bool {
	return isExamplesPackage(pkgPath) || isGeneratedAPIPackage(pkgPath)
}

func isExamplesPackage(pkgPath string) bool {
	return strings.Contains(pkgPath, "/examples/") || strings.HasSuffix(pkgPath, "/examples")
}

func isGeneratedAPIPackage(pkgPath string) bool {
	return strings.Contains(pkgPath, "/api/") || strings.HasSuffix(pkgPath, "/api")
}

type diagnostic struct {
	lint    string
	key     string
	message string
	file    string
	line    int
	notes   []string
	picture string
}

func (d diagnostic) Format() string {
	var b strings.Builder
	loc := ""
	if d.file != "" {
		loc = fmt.Sprintf("\n  --> %s:%d", d.file, d.line)
	}
	fmt.Fprintf(&b, "error[%s]: %s%s\n", d.lint, d.message, loc)
	for _, n := range d.notes {
		fmt.Fprintf(&b, "  = note: %s\n", n)
	}
	if d.picture != "" {
		b.WriteString(d.picture)
		if !strings.HasSuffix(d.picture, "\n") {
			b.WriteByte('\n')
		}
	}
	b.WriteByte('\n')
	return b.String()
}

func relPath(path string) string {
	for _, seg := range []string{"/dst/", "/controlplane/", "/broker/", "/shared/"} {
		if i := strings.Index(path, seg); i >= 0 {
			return path[i+1:]
		}
	}
	return path
}

func shortPkg(path string) string {
	return strings.TrimPrefix(path, mdsPrefix)
}

func isTestPackage(p *packages.Package) bool {
	return strings.HasSuffix(p.PkgPath, "_test") || strings.HasSuffix(p.Name, "_test")
}

func posLine(fset *token.FileSet, pos token.Pos) (file string, line int) {
	if !pos.IsValid() || fset == nil {
		return "", 0
	}
	p := fset.Position(pos)
	return relPath(p.Filename), p.Line
}

func fsetOf(pkgs []*packages.Package, pos token.Pos) *token.FileSet {
	for _, p := range pkgs {
		if p.Fset != nil && pos.IsValid() {
			if f := p.Fset.File(pos); f != nil {
				return p.Fset
			}
		}
	}
	for _, p := range pkgs {
		if p.Fset != nil {
			return p.Fset
		}
	}
	return nil
}
