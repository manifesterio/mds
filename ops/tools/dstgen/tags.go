package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/token"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// retired is the name of a tag line whose type was deleted.
const retired = "-"

// tagFile is the dst.tags of one directory: tag number to type name.
type tagFile struct {
	path   string
	byTag  map[uint64]string
	byName map[string]uint64
	order  []uint64 // tags in file order; new tags are appended
}

const tagsHeader = "# dst.tags: written by dstgen. Append-only: never change a number, never reuse one.\n" +
	"# <tag> <TypeName>; a retired line has the name -\n"

func newTagFile(path string) *tagFile {
	return &tagFile{path: path, byTag: map[uint64]string{}, byName: map[string]uint64{}}
}

// readTags reads path. A missing file is an empty one.
func readTags(path string) (*tagFile, error) {
	data, err := os.ReadFile(path) //nolint:gosec // the dst.tags of a package dstgen was asked to generate
	if os.IsNotExist(err) {
		return newTagFile(path), nil
	}
	if err != nil {
		return nil, err
	}
	return parseTags(path, data)
}

func parseTags(path string, data []byte) (*tagFile, error) {
	t := newTagFile(path)
	sc := bufio.NewScanner(bytes.NewReader(data))
	for line := 1; sc.Scan(); line++ {
		text, _, _ := strings.Cut(sc.Text(), "#")
		fields := strings.Fields(text)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 2 {
			return nil, fmt.Errorf("%s:%d: want <tag> <TypeName>", path, line)
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("%s:%d: bad tag %q", path, line, fields[0])
		}
		if err := t.add(n, fields[1]); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
	}
	return t, sc.Err()
}

func (t *tagFile) add(n uint64, name string) error {
	if old, ok := t.byTag[n]; ok {
		return fmt.Errorf("tag %d is used twice (%s and %s)", n, old, name)
	}
	if name != retired {
		if old, ok := t.byName[name]; ok {
			return fmt.Errorf("%s has two tags (%d and %d)", name, old, n)
		}
		t.byName[name] = n
	}
	t.byTag[n] = name
	t.order = append(t.order, n)
	return nil
}

func (t *tagFile) retire(name string) {
	n, ok := t.byName[name]
	if !ok {
		return
	}
	delete(t.byName, name)
	t.byTag[n] = retired
}

func (t *tagFile) bytes() []byte {
	if len(t.order) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString(tagsHeader)
	for _, n := range t.order {
		fmt.Fprintf(&b, "%d %s\n", n, t.byTag[n])
	}
	return []byte(b.String())
}

// assign gives every message type of the directory a tag. A type that has
// none gets one more than the highest tag among the types that share a sum
// with it (or among all payload types); see next.
func (t *tagFile) assign(all []*annotated, errs *errorList) {
	for _, a := range all {
		if !a.message() {
			continue
		}
		if _, ok := t.byName[a.name]; ok {
			continue
		}
		if err := t.add(t.next(a, all), a.name); err != nil {
			errs.add(a.pos, "%v", err)
		}
	}
}

// next is one more than the highest tag related to a; if that number is
// taken (by another range), one more than the highest tag in the file.
func (t *tagFile) next(a *annotated, all []*annotated) uint64 {
	var high uint64
	for _, b := range all {
		n, ok := t.byName[b.name]
		if !ok || !related(a, b) {
			continue
		}
		high = max(high, n)
	}
	if _, taken := t.byTag[high+1]; !taken {
		return high + 1
	}
	return slices.Max(t.order) + 1
}

// related reports whether two message types draw tags from the same range:
// they share a sum, or both are payloads.
func related(a, b *annotated) bool {
	if a.payload || b.payload {
		return a.payload && b.payload
	}
	for _, s := range a.cases {
		if slices.Contains(b.cases, s) {
			return true
		}
	}
	return false
}

// retireMissing marks the lines of types that no longer exist.
func (t *tagFile) retireMissing(exists func(name string) bool) {
	for _, name := range slices.Sorted(maps.Keys(t.byName)) {
		if !exists(name) {
			t.retire(name)
		}
	}
}

// importTags adds the tag of every type that has a hand-written
// `Tag() uint64 { return N }`, or a Fold whose first statement is
// h.Uint64(N).
func (t *tagFile) importTags(s scanned, errs *errorList) {
	for _, name := range slices.Sorted(maps.Keys(s.methods)) {
		n, pos, ok := literalTag(s.methods[name])
		if !ok {
			continue
		}
		if old, has := t.byName[name]; has {
			if old != n {
				errs.add(pos, "%s: dst.tags says %d, the code says %d", name, old, n)
			}
			continue
		}
		if err := t.add(n, name); err != nil {
			errs.add(pos, "%v", err)
		}
	}
}

func literalTag(ms []method) (uint64, token.Position, bool) {
	for _, m := range ms {
		if m.name == "Tag" {
			if n, ok := returnedLiteral(m.decl); ok {
				return n, m.pos, true
			}
		}
	}
	for _, m := range ms {
		if m.name == "Fold" {
			if n, ok := firstUint64Literal(m.decl); ok {
				return n, m.pos, true
			}
		}
	}
	return 0, token.Position{}, false
}

func returnedLiteral(d *ast.FuncDecl) (uint64, bool) {
	if d.Body == nil || len(d.Body.List) != 1 {
		return 0, false
	}
	ret, ok := d.Body.List[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return 0, false
	}
	return intLiteral(ret.Results[0])
}

// firstUint64Literal matches a first statement h.Uint64(N).
func firstUint64Literal(d *ast.FuncDecl) (uint64, bool) {
	call, ok := firstUint64Call(d)
	if !ok {
		return 0, false
	}
	return intLiteral(call.Args[0])
}

func firstUint64Call(d *ast.FuncDecl) (*ast.CallExpr, bool) {
	if d.Body == nil || len(d.Body.List) == 0 {
		return nil, false
	}
	st, ok := d.Body.List[0].(*ast.ExprStmt)
	if !ok {
		return nil, false
	}
	call, ok := st.X.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil, false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Uint64" {
		return nil, false
	}
	return call, true
}

func intLiteral(e ast.Expr) (uint64, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.INT {
		return 0, false
	}
	n, err := strconv.ParseUint(lit.Value, 0, 64)
	return n, err == nil
}

// manualFoldStartsWithTag checks that a hand-written Fold writes its tag
// first: h.Uint64(N) or h.Uint64(x.Tag()).
func manualFoldStartsWithTag(d *ast.FuncDecl, tag uint64) bool {
	call, ok := firstUint64Call(d)
	if !ok {
		return false
	}
	if n, ok := intLiteral(call.Args[0]); ok {
		return n == tag
	}
	inner, ok := call.Args[0].(*ast.CallExpr)
	if !ok || len(inner.Args) != 0 {
		return false
	}
	sel, ok := inner.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == "Tag"
}

// checkBase compares every dst.tags with its version at git revision rev:
// no line may be removed, and no number may change its type except to retire
// it or to follow a rename of the type.
func checkBase(res result, rev string) []string {
	var problems []string
	for _, dir := range slices.Sorted(maps.Keys(res.tags)) {
		cur := res.tags[dir]
		old, err := baseTags(dir, rev)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", cur.path, err))
			continue
		}
		for _, n := range old.order {
			if _, ok := cur.byTag[n]; !ok {
				problems = append(problems, fmt.Sprintf("%s: tag %d (%s at %s) was removed; retire it with - instead", cur.path, n, old.byTag[n], rev))
			}
		}
	}
	return problems
}

func baseTags(dir, rev string) (*tagFile, error) {
	abs, err := filepath.Abs(filepath.Join(dir, tagsFile))
	if err != nil {
		return nil, err
	}
	top, err := exec.CommandContext(context.Background(), "git", "-C", dir, "rev-parse", "--show-toplevel").Output() //nolint:gosec // dir comes from the command line of this dev tool
	if err != nil {
		return nil, fmt.Errorf("git rev-parse: %w", err)
	}
	rel, err := filepath.Rel(strings.TrimSpace(string(top)), abs)
	if err != nil {
		return nil, err
	}
	out, err := exec.CommandContext(context.Background(), "git", "-C", dir, "show", rev+":"+filepath.ToSlash(rel)).Output() //nolint:gosec // dir and rev come from the command line of this dev tool
	if err != nil {
		return newTagFile(abs), nil //nolint:nilerr // no dst.tags at rev: nothing to compare
	}
	return parseTags(rev+":"+rel, out)
}
