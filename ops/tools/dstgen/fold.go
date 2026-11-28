package main

import (
	"fmt"
	"go/token"
	"go/types"
	"strings"
)

// folder writes the statements that fold one value into h. It knows which
// types of the unit get a generated Fold, so a field of such a type folds
// through it even before the generated file exists.
type folder struct {
	pkg       *types.Package
	generated map[string]bool // "pkgpath.Name" of every type that gets a generated Fold
	imports   map[string]bool // import paths the written code needs
	b         *strings.Builder
	depth     int
	locals    int // counter for fresh variable names
	pos       token.Position
	errs      *errorList
	what      string // "T.Field" for error messages
}

func (f *folder) line(format string, args ...any) {
	f.b.WriteString(strings.Repeat("\t", f.depth+1))
	fmt.Fprintf(f.b, format, args...)
	f.b.WriteString("\n")
}

func (f *folder) fail(format string, args ...any) {
	f.errs.add(f.pos, "%s: "+format, append([]any{f.what}, args...)...)
}

// fold writes code that folds expr of type t. addressable says whether
// expr[:] is legal (for byte arrays).
func (f *folder) fold(expr string, t types.Type, addressable bool) {
	if f.hasFold(t) {
		f.foldMethod(expr, t)
		return
	}
	switch u := t.Underlying().(type) {
	case *types.Basic:
		f.foldBasic(expr, t, u)
	case *types.Slice:
		f.foldSlice(expr, u)
	case *types.Array:
		f.foldArray(expr, u, addressable)
	case *types.Map:
		f.foldMap(expr, u)
	case *types.Struct:
		f.foldStruct(expr, u, addressable)
	case *types.Interface:
		f.fail("interface %s has no Fold(*trace.Hash)", types.TypeString(t, f.qualifier))
	default:
		f.fail("cannot fold a %s", types.TypeString(t, f.qualifier))
	}
}

func (f *folder) qualifier(p *types.Package) string {
	if p == f.pkg {
		return ""
	}
	return p.Name()
}

// hasFold reports whether values of t have Fold(*trace.Hash), now or once
// dstgen has written it.
func (f *folder) hasFold(t types.Type) bool {
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil && f.generated[typeKey(n.Obj())] {
		return true
	}
	return hasFold(t)
}

func (f *folder) foldMethod(expr string, t types.Type) {
	if types.IsInterface(t) {
		f.line("if %s == nil {", expr)
		f.line("\th.Uint64(0)")
		f.line("} else {")
		f.line("\t%s.Fold(h)", expr)
		f.line("}")
		return
	}
	f.line("%s.Fold(h)", expr)
}

func (f *folder) foldBasic(expr string, t types.Type, u *types.Basic) {
	info := u.Info()
	switch {
	case info&types.IsBoolean != 0:
		f.line("h.Bool(%s)", convert("bool", expr, t))
	case info&types.IsString != 0:
		f.line("h.String(%s)", convert("string", expr, t))
	case info&types.IsUnsigned != 0 && u.Kind() != types.Uintptr:
		f.line("h.Uint64(%s)", convert("uint64", expr, t))
	case info&types.IsInteger != 0 && u.Kind() != types.Uintptr:
		f.line("h.Int64(%s)", convert("int64", expr, t))
	case info&types.IsFloat != 0:
		f.imports["math"] = true
		f.line("h.Uint64(math.Float64bits(%s))", convert("float64", expr, t))
	default:
		f.fail("cannot fold a %s", u.Name())
	}
}

// convert wraps expr in a conversion to the basic type want, unless it
// already has that type.
func convert(want, expr string, t types.Type) string {
	if b, ok := t.(*types.Basic); ok && b.Name() == want {
		return expr
	}
	return want + "(" + expr + ")"
}

func isByte(t types.Type) bool {
	b, ok := t.Underlying().(*types.Basic)
	return ok && b.Kind() == types.Uint8
}

func (f *folder) foldSlice(expr string, u *types.Slice) {
	if isByte(u.Elem()) && !f.hasFold(u.Elem()) {
		f.line("h.Bytes(%s)", expr)
		return
	}
	f.line("h.Uint64(uint64(len(%s)))", expr)
	f.loop(expr, u.Elem())
}

func (f *folder) foldArray(expr string, u *types.Array, addressable bool) {
	if isByte(u.Elem()) && !f.hasFold(u.Elem()) {
		if !addressable {
			v := f.local("a")
			f.line("%s := %s", v, expr)
			expr = v
		}
		f.line("h.Bytes(%s[:])", expr)
		return
	}
	f.line("h.Uint64(%d)", u.Len())
	f.loop(expr, u.Elem())
}

func (f *folder) loop(expr string, elem types.Type) {
	v := f.local("e")
	f.line("for _, %s := range %s {", v, expr)
	f.depth++
	f.fold(v, elem, true)
	f.depth--
	f.line("}")
}

func (f *folder) foldMap(expr string, u *types.Map) {
	key, ok := u.Key().Underlying().(*types.Basic)
	if !ok || key.Info()&types.IsOrdered == 0 {
		f.fail("map keys must be ordered (string, integer or float) to fold in sorted order")
		return
	}
	f.imports["maps"], f.imports["slices"] = true, true
	k, v := f.local("k"), f.local("v")
	f.line("h.Uint64(uint64(len(%s)))", expr)
	f.line("for _, %s := range slices.Sorted(maps.Keys(%s)) {", k, expr)
	f.depth++
	f.fold(k, u.Key(), true)
	f.line("%s := %s[%s]", v, expr, k)
	f.fold(v, u.Elem(), true)
	f.depth--
	f.line("}")
}

func (f *folder) foldStruct(expr string, u *types.Struct, addressable bool) {
	for fl := range u.Fields() {
		if !fl.Exported() && fl.Pkg() != f.pkg {
			f.fail("field %s of another package is unexported", fl.Name())
			continue
		}
		if fl.Name() == "_" {
			continue
		}
		f.fold(expr+"."+fl.Name(), fl.Type(), addressable)
	}
}

// local returns a fresh variable name.
func (f *folder) local(prefix string) string {
	f.locals++
	return fmt.Sprintf("%s%d", prefix, f.locals)
}

// hasFold reports whether the method set of t has Fold(h *trace.Hash) with
// no results.
func hasFold(t types.Type) bool {
	obj, _, _ := types.LookupFieldOrMethod(t, false, nil, "Fold")
	fn, ok := obj.(*types.Func)
	if !ok {
		return false
	}
	sig := fn.Signature()
	if sig.Params().Len() != 1 || sig.Results().Len() != 0 {
		return false
	}
	ptr, ok := sig.Params().At(0).Type().(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	return ok && named.Obj().Name() == "Hash" && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == tracePath
}

// typeKey names a type across packages: its package path and name.
func typeKey(obj *types.TypeName) string { return obj.Pkg().Path() + "." + obj.Name() }
