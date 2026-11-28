package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
)

// annotated is one type with dstgen annotations.
type annotated struct {
	name     string
	obj      *types.TypeName
	pos      token.Position
	sum      bool     // //dst:sum
	open     bool     // //dst:sum open: no exhaustiveness check (no //sumtype:decl)
	cases    []string // //dst:cases: the sums this type belongs to
	payload  bool     // //dst:payload
	answer   string   // //dst:answer: the answer type name
	manual   bool     // //dst:fold manual
	state    bool     // //dst:state
	sumDecl  bool     // the doc has //sumtype:decl
	hasNotes bool
}

// message reports whether dstgen writes Tag (and usually Fold) for the type.
func (a *annotated) message() bool { return len(a.cases) > 0 || a.payload }

// scanned is what one unit declares.
type scanned struct {
	types   []*annotated          // in declaration order
	byName  map[string]*annotated // every annotated type
	methods map[string][]method   // hand-written methods, by receiver type name
}

// method is one hand-written method.
type method struct {
	name string
	decl *ast.FuncDecl
	pos  token.Position
}

// scan reads the annotations and methods of one unit.
func scan(u unit, errs *errorList) scanned {
	s := scanned{byName: map[string]*annotated{}, methods: map[string][]method{}}
	for _, f := range u.files {
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				s.scanTypes(u, d, errs)
			case *ast.FuncDecl:
				s.scanMethod(u, d)
			}
		}
	}
	return s
}

func (s *scanned) scanTypes(u unit, d *ast.GenDecl, errs *errorList) {
	if d.Tok != token.TYPE {
		return
	}
	for _, spec := range d.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		a := &annotated{name: ts.Name.Name, pos: u.pkg.Fset.Position(ts.Pos())}
		if obj, ok := u.pkg.TypesInfo.Defs[ts.Name].(*types.TypeName); ok {
			a.obj = obj
		}
		a.read(d.Doc, errs, u.pkg.Fset)
		if len(d.Specs) > 1 || d.Lparen.IsValid() {
			a.read(ts.Doc, errs, u.pkg.Fset)
		}
		if !a.hasNotes {
			continue
		}
		if ts.TypeParams != nil {
			errs.add(a.pos, "%s: dstgen annotations on a generic type are not supported", a.name)
			continue
		}
		s.types = append(s.types, a)
		s.byName[a.name] = a
	}
}

func (s *scanned) scanMethod(u unit, d *ast.FuncDecl) {
	if d.Recv == nil || len(d.Recv.List) != 1 {
		return
	}
	t := d.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}
	id, ok := t.(*ast.Ident)
	if !ok {
		return
	}
	s.methods[id.Name] = append(s.methods[id.Name], method{name: d.Name.Name, decl: d, pos: u.pkg.Fset.Position(d.Pos())})
}

// read applies the annotations of one doc comment.
func (a *annotated) read(doc *ast.CommentGroup, errs *errorList, fset *token.FileSet) {
	if doc == nil {
		return
	}
	for _, c := range doc.List {
		text := strings.TrimSpace(c.Text)
		if text == "//sumtype:decl" {
			a.sumDecl = true
			continue
		}
		rest, ok := strings.CutPrefix(text, "//dst:")
		if !ok {
			continue
		}
		a.hasNotes = true
		if err := a.apply(strings.Fields(rest)); err != nil {
			errs.add(fset.Position(c.Pos()), "%s: %v", a.name, err)
		}
	}
}

func (a *annotated) apply(words []string) error {
	if len(words) == 0 {
		return errors.New("empty //dst: annotation")
	}
	kind, args := words[0], words[1:]
	want := map[string]int{"payload": 0, "state": 0, "answer": 1, "fold": 1}
	if n, fixed := want[kind]; fixed && len(args) != n {
		return fmt.Errorf("//dst:%s takes %d argument(s), got %d", kind, n, len(args))
	}
	switch kind {
	case "sum":
		switch {
		case len(args) == 1 && args[0] == "open":
			a.open = true
		case len(args) != 0:
			return fmt.Errorf("//dst:sum takes no argument or open, got %q", strings.Join(args, " "))
		}
		a.sum = true
	case "cases":
		if len(args) == 0 {
			return errors.New("//dst:cases needs at least one sum")
		}
		a.cases = append(a.cases, args...)
	case "payload":
		a.payload = true
	case "answer":
		a.answer = args[0]
	case "fold":
		if args[0] != "manual" {
			return fmt.Errorf("unknown //dst:fold %q (only manual)", args[0])
		}
		a.manual = true
	case "state":
		a.state = true
	default:
		return fmt.Errorf("unknown annotation //dst:%s", kind)
	}
	return nil
}

// validate checks the annotations of a unit against its types.
func (s *scanned) validate(errs *errorList) {
	for _, a := range s.types {
		s.validateOne(a, errs)
	}
}

func (s *scanned) validateOne(a *annotated, errs *errorList) {
	switch {
	case a.obj == nil:
		errs.add(a.pos, "%s: no type information", a.name)
		return
	case a.sum && (a.message() || a.state):
		errs.add(a.pos, "%s: //dst:sum cannot be combined with other annotations", a.name)
	case len(a.cases) > 0 && a.payload:
		errs.add(a.pos, "%s: a type is either //dst:cases or //dst:payload", a.name)
	case (a.manual || a.answer != "") && !a.message():
		errs.add(a.pos, "%s: //dst:fold and //dst:answer need //dst:cases or //dst:payload", a.name)
	case a.answer != "" && len(a.cases) != 1:
		errs.add(a.pos, "%s: //dst:answer needs exactly one sum (the IO sum)", a.name)
	}
	if a.sum {
		s.validateSum(a, errs)
	}
	for _, sum := range a.cases {
		if t := s.byName[sum]; t == nil || !t.sum {
			errs.add(a.pos, "%s: //dst:cases %s: %s is not a //dst:sum of this package", a.name, sum, sum)
		}
	}
	if a.state {
		if _, ok := a.obj.Type().Underlying().(*types.Struct); !ok {
			errs.add(a.pos, "%s: //dst:state needs a struct", a.name)
		}
	}
	s.checkHandWritten(a, errs)
}

func (s *scanned) validateSum(a *annotated, errs *errorList) {
	iface, ok := a.obj.Type().Underlying().(*types.Interface)
	if !ok {
		errs.add(a.pos, "%s: //dst:sum needs an interface", a.name)
		return
	}
	switch {
	case !a.open && !a.sumDecl:
		errs.add(a.pos, "%s: a //dst:sum also needs //sumtype:decl (for the exhaustive switch linter), or use //dst:sum open", a.name)
	case a.open && a.sumDecl:
		errs.add(a.pos, "%s: //dst:sum open means no exhaustiveness check: remove //sumtype:decl", a.name)
	}
	if !hasFold(iface) {
		errs.add(a.pos, "%s: a //dst:sum must embed actor.Payload: it has no Fold(*trace.Hash)", a.name)
	}
	if marker(iface, a.name) == nil {
		errs.add(a.pos, "%s: a //dst:sum needs the marker method %s()", a.name, markerName(a.name))
	}
}

// checkHandWritten reports hand-written methods that dstgen now writes.
func (s *scanned) checkHandWritten(a *annotated, errs *errorList) {
	generated := map[string]bool{}
	if a.message() {
		generated["Tag"] = true
		generated["Fold"] = !a.manual
		for _, sum := range a.cases {
			generated[markerName(sum)] = true
		}
	}
	if a.answer != "" {
		generated["AsRequest"] = true
	}
	if a.state {
		generated["StateHash"] = true
	}
	for _, m := range s.methods[a.name] {
		if generated[m.name] {
			errs.add(m.pos, "%s.%s is written by dstgen: delete the hand-written method", a.name, m.name)
		}
	}
	if a.manual && !slices.ContainsFunc(s.methods[a.name], func(m method) bool { return m.name == "Fold" }) {
		errs.add(a.pos, "%s: //dst:fold manual but there is no hand-written Fold", a.name)
	}
}

func markerName(sum string) string {
	if sum == "" {
		return "is"
	}
	return "is" + strings.ToUpper(sum[:1]) + sum[1:]
}

// marker returns the marker method a sum declares itself (not embedded).
func marker(iface *types.Interface, sum string) *types.Func {
	for m := range iface.ExplicitMethods() {
		if m.Name() == markerName(sum) && m.Signature().Params().Len() == 0 && m.Signature().Results().Len() == 0 {
			return m
		}
	}
	return nil
}
