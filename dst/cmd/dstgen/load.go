package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"
)

// The generated files. Test types go to test files, so they are only
// compiled with the tests.
const (
	genFile      = "zz_dst_gen.go"
	genTestFile  = "zz_dst_gen_test.go"   // types declared in _test.go files of the package
	genXTestFile = "zz_dst_gen_x_test.go" // types of the external test package (pkg_test)
	tagsFile     = "dst.tags"
	tracePath    = "github.com/manifesterio/mds/dst/core/trace"
)

func isGenerated(path string) bool {
	switch filepath.Base(path) {
	case genFile, genTestFile, genXTestFile:
		return true
	}
	return false
}

// unit is the part of one directory that goes into one generated file: the
// package itself, the _test.go files of the package, or the external test
// package.
type unit struct {
	dir     string
	file    string // the generated file name
	pkg     *packages.Package
	files   []*ast.File // the non-generated files of this unit
	pkgName string
}

func (u unit) outPath() string { return filepath.Join(u.dir, u.file) }

// load loads the packages of patterns with their tests. Generated files are
// replaced by an empty package clause, so stale output never feeds back into
// generation.
func load(patterns []string) ([]unit, error) {
	overlay, err := blankGenerated(patterns)
	if err != nil {
		return nil, err
	}
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests:   true,
		Overlay: overlay,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}
	var units []unit
	seen := map[string]bool{}
	for _, p := range pkgs {
		if u, ok := unitOf(p); ok && !seen[u.outPath()] {
			seen[u.outPath()] = true
			units = append(units, u)
		}
	}
	if len(units) == 0 {
		return nil, errors.New("no packages matched " + strings.Join(patterns, " "))
	}
	slices.SortFunc(units, func(a, b unit) int { return strings.Compare(a.outPath(), b.outPath()) })
	return units, nil
}

// unitOf picks the files of p that belong to one unit. go/packages returns
// three variants per directory with tests: the package, the package with its
// test files ("p [p.test]"), and the external test package.
func unitOf(p *packages.Package) (unit, bool) {
	if len(p.Syntax) == 0 || strings.HasSuffix(p.ID, ".test") {
		return unit{}, false
	}
	dir := filepath.Dir(p.Fset.File(p.Syntax[0].Pos()).Name())
	u := unit{dir: dir, pkg: p, pkgName: p.Name}
	testVariant := strings.Contains(p.ID, " [")
	if testVariant && !strings.HasSuffix(p.Name, "_test") && p.ID != p.PkgPath+" ["+p.PkgPath+".test]" {
		return unit{}, false // the package recompiled for another package's tests: a copy
	}
	switch {
	case strings.HasSuffix(p.Name, "_test"):
		u.file = genXTestFile
	case testVariant:
		u.file = genTestFile
	default:
		u.file = genFile
	}
	for _, f := range p.Syntax {
		name := p.Fset.File(f.Pos()).Name()
		if isGenerated(name) {
			continue
		}
		isTest := strings.HasSuffix(name, "_test.go")
		if u.file == genTestFile && !isTest {
			continue // the package's own files are handled by the plain variant
		}
		u.files = append(u.files, f)
	}
	return u, true
}

// blankGenerated finds the generated files under the directories of
// patterns and maps each to a bare package clause.
func blankGenerated(patterns []string) (map[string][]byte, error) {
	overlay := map[string][]byte{}
	for _, pat := range patterns {
		root, recursive := strings.CutSuffix(pat, "/...")
		err := walkDirs(root, recursive, func(dir string) error {
			return blankDir(dir, overlay)
		})
		if err != nil {
			return nil, err
		}
	}
	return overlay, nil
}

func walkDirs(root string, recursive bool, fn func(dir string) error) error {
	if !recursive {
		return fn(root)
	}
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		base := d.Name()
		if path != root && (base == "testdata" || strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_")) {
			return filepath.SkipDir
		}
		return fn(path)
	})
}

func blankDir(dir string, overlay map[string][]byte) error {
	for _, name := range []string{genFile, genTestFile, genXTestFile} {
		path, err := filepath.Abs(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.PackageClauseOnly)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		overlay[path] = []byte("package " + f.Name.Name + "\n")
	}
	return nil
}
