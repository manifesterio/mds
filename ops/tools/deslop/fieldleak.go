package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

type fieldUse struct {
	pkg  string
	file string
	line int
}

func checkFieldLeak(pkgs []*packages.Package) []diagnostic {
	prod := productionPackages(pkgs)

	type structInfo struct {
		obj     *types.TypeName
		fields  []*types.Var
		pkgPath string
	}
	structs := map[*types.TypeName]*structInfo{}

	for _, p := range prod {
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			obj, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || !obj.Exported() {
				continue
			}
			named, ok := obj.Type().(*types.Named)
			if !ok {
				continue
			}
			st, ok := named.Underlying().(*types.Struct)
			if !ok {
				continue
			}
			var fields []*types.Var
			for i := range st.NumFields() {
				f := st.Field(i)
				if f.Anonymous() {
					continue
				}
				fields = append(fields, f)
			}
			if len(fields) == 0 {
				continue
			}
			if types.NewMethodSet(types.NewPointer(named)).Len() == 0 {
				continue
			}
			structs[obj] = &structInfo{obj: obj, fields: fields, pkgPath: p.PkgPath}
		}
	}

	outside := map[*types.Var][]fieldUse{}
	touched := map[*types.Var]bool{}
	written := map[*types.Var]bool{}

	for _, p := range pkgs {
		if p.TypesInfo == nil {
			continue
		}
		for _, file := range p.Syntax {
			ast.Inspect(file, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.SelectorExpr:
					sel := p.TypesInfo.Selections[node]
					if sel == nil || sel.Kind() != types.FieldVal {
						return true
					}
					field, ok := sel.Obj().(*types.Var)
					if !ok || field.Pkg() == nil {
						return true
					}
					if field.Pkg().Path() == p.PkgPath {
						return true
					}
					if !inMDS(p.PkgPath) {
						return true
					}
					touched[field] = true
					fset := p.Fset
					fileName, line := posLine(fset, node.Pos())
					if isWriteUse(file, node) {
						written[field] = true
						outside[field] = append(outside[field], fieldUse{pkg: p.PkgPath, file: fileName, line: line})
					}
				case *ast.CompositeLit:
					tv, ok := p.TypesInfo.Types[node]
					if !ok {
						return true
					}
					named := namedStruct(tv.Type)
					if named == nil || named.Obj().Pkg() == nil {
						return true
					}
					if named.Obj().Pkg().Path() == p.PkgPath {
						return true
					}
					if !inMDS(p.PkgPath) {
						return true
					}
					st, _ := named.Underlying().(*types.Struct)
					if st == nil {
						return true
					}
					for _, elt := range node.Elts {
						kv, ok := elt.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						id, ok := kv.Key.(*ast.Ident)
						if !ok {
							continue
						}
						for i := range st.NumFields() {
							f := st.Field(i)
							if f.Name() != id.Name {
								continue
							}
							touched[f] = true
							written[f] = true
							fileName, line := posLine(p.Fset, id.Pos())
							outside[f] = append(outside[f], fieldUse{pkg: p.PkgPath, file: fileName, line: line})
						}
					}
				}
				return true
			})
		}
	}

	var diags []diagnostic
	for _, info := range structs {
		kept := false
		for _, f := range info.fields {
			if !touched[f] {
				kept = true
				break
			}
		}
		if !kept {
			continue
		}
		var leaked []*types.Var
		for _, f := range info.fields {
			if written[f] && len(outside[f]) > 0 {
				leaked = append(leaked, f)
			}
		}
		if len(leaked) == 0 {
			continue
		}
		sort.Slice(leaked, func(i, j int) bool { return leaked[i].Name() < leaked[j].Name() })
		for _, field := range leaked {
			uses := outside[field]
			byPkg := map[string]int{}
			for _, u := range uses {
				byPkg[u.pkg]++
			}
			type pk struct {
				n int
				p string
			}
			var pkgsCounts []pk
			for pth, n := range byPkg {
				pkgsCounts = append(pkgsCounts, pk{n: n, p: pth})
			}
			sort.Slice(pkgsCounts, func(i, j int) bool {
				if pkgsCounts[i].n != pkgsCounts[j].n {
					return pkgsCounts[i].n > pkgsCounts[j].n
				}
				return pkgsCounts[i].p < pkgsCounts[j].p
			})

			var keptNames, otherLeaked []string
			for _, f := range info.fields {
				if !touched[f] {
					keptNames = append(keptNames, f.Name())
				} else if f != field && written[f] {
					otherLeaked = append(otherLeaked, f.Name())
				}
			}
			sort.Strings(keptNames)
			sort.Strings(otherLeaked)

			notes := []string{}
			for _, u := range uses {
				if len(notes) >= heldInMind {
					notes = append(notes, fmt.Sprintf("... %d more places", len(uses)-heldInMind))
					break
				}
				notes = append(notes, fmt.Sprintf("%s:%d: `%s`", u.file, u.line, shortPkg(u.pkg)))
			}

			var pic strings.Builder
			fmt.Fprintf(&pic, "    %s.%s\n", shortPkg(info.pkgPath), info.obj.Name())
			fmt.Fprintf(&pic, "      %s written from outside:", field.Name())
			for i, pc := range pkgsCounts {
				if i == 0 {
					fmt.Fprintf(&pic, " %d %s\n", pc.n, shortPkg(pc.p))
				} else {
					fmt.Fprintf(&pic, "         %d %s\n", pc.n, shortPkg(pc.p))
				}
				if i+1 >= heldInMind {
					break
				}
			}
			if len(otherLeaked) > 0 {
				fmt.Fprintf(&pic, "      also known outside: %s\n", strings.Join(otherLeaked, ", "))
			}
			if len(keptNames) > 0 {
				fmt.Fprintf(&pic, "      kept: %s\n", strings.Join(keptNames, ", "))
			}

			file, line := "", 0
			if fset := fsetOf(pkgs, field.Pos()); fset != nil {
				file, line = posLine(fset, field.Pos())
			}
			diags = append(diags, diagnostic{
				lint: "field-leak",
				key:  info.pkgPath + "." + info.obj.Name() + "." + field.Name(),
				message: fmt.Sprintf("`%s.%s` is known outside `%s`: %d places in %d packages",
					info.obj.Name(), field.Name(), shortPkg(info.pkgPath), len(uses), len(byPkg)),
				file:    file,
				line:    line,
				notes:   notes,
				picture: pic.String(),
			})
		}
	}
	return diags
}

func namedStruct(t types.Type) *types.Named {
	switch t := t.(type) {
	case *types.Named:
		if _, ok := t.Underlying().(*types.Struct); ok {
			return t
		}
	case *types.Pointer:
		return namedStruct(t.Elem())
	}
	return nil
}

func isWriteUse(file *ast.File, sel *ast.SelectorExpr) bool {
	var path []ast.Node
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			if len(path) > 0 {
				path = path[:len(path)-1]
			}
			return false
		}
		path = append(path, n)
		if n != sel {
			return true
		}
		for i := len(path) - 2; i >= 0; i-- {
			switch p := path[i].(type) {
			case *ast.ParenExpr:
				continue
			case *ast.UnaryExpr:
				return p.Op == token.AND
			case *ast.IncDecStmt:
				return true
			case *ast.AssignStmt:
				for _, lhs := range p.Lhs {
					if exprContains(lhs, sel) {
						return true
					}
				}
				return false
			default:
				return false
			}
		}
		return false
	})
	return false
}

func exprContains(e ast.Expr, target ast.Node) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if n == target {
			found = true
			return false
		}
		return true
	})
	return found
}
