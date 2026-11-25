package main

import (
	"fmt"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

func checkShallow(pkgs []*packages.Package) []diagnostic {
	prod := productionPackages(pkgs)
	defs := exportedFuncs(prod)
	shown := map[types.Object]map[string]int{}

	for _, p := range pkgs {
		if p.TypesInfo == nil {
			continue
		}
		for _, obj := range p.TypesInfo.Uses {
			if obj == nil || !obj.Exported() {
				continue
			}
			fn, ok := obj.(*types.Func)
			if !ok {
				continue
			}
			if _, mine := defs[fn]; !mine {
				continue
			}
			if fn.Pkg() == nil || fn.Pkg().Path() == p.PkgPath {
				continue
			}
			if !inMDS(p.PkgPath) {
				continue
			}
			if shown[fn] == nil {
				shown[fn] = map[string]int{}
			}
			shown[fn][p.PkgPath]++
		}
	}

	byPkg := map[string][]types.Object{}
	for fn := range defs {
		pkg := fn.Pkg().Path()
		byPkg[pkg] = append(byPkg[pkg], fn)
	}

	var diags []diagnostic
	for pkgPath, fns := range byPkg {
		if len(fns) == 0 {
			continue
		}
		var shownFns []types.Object
		var hiddenFns []types.Object
		for _, fn := range fns {
			if len(shown[fn]) > 0 {
				shownFns = append(shownFns, fn)
			} else {
				hiddenFns = append(hiddenFns, fn)
			}
		}
		if len(shownFns) < heldInMind {
			continue
		}
		if len(shownFns) < len(hiddenFns)*breakEven {
			continue
		}
		groups := map[string][]string{}
		for _, fn := range shownFns {
			groups[thingName(fn)] = append(groups[thingName(fn)], fn.Name())
		}
		type list struct {
			name  string
			items []string
		}
		var lists []list
		for name, items := range groups {
			sort.Strings(items)
			lists = append(lists, list{name: name, items: items})
		}
		sort.Slice(lists, func(i, j int) bool {
			if len(lists[i].items) != len(lists[j].items) {
				return len(lists[i].items) > len(lists[j].items)
			}
			return lists[i].name < lists[j].name
		})
		wide := false
		for _, l := range lists {
			if len(l.items) >= heldInMind {
				wide = true
				break
			}
		}
		if !wide && len(shownFns) >= heldInMind {
			wide = true
		}
		if !wide {
			continue
		}

		sort.Slice(shownFns, func(i, j int) bool { return shownFns[i].Name() < shownFns[j].Name() })
		notes := []string{
			fmt.Sprintf("shows %d working functions, hides %d", len(shownFns), len(hiddenFns)),
		}
		for _, l := range lists {
			if len(l.items) > heldInMind {
				notes = append(notes, fmt.Sprintf("%s: %s, +%d more", l.name, strings.Join(l.items[:heldInMind], ", "), len(l.items)-heldInMind))
			} else {
				notes = append(notes, fmt.Sprintf("%s: %s", l.name, strings.Join(l.items, ", ")))
			}
			if len(notes) > heldInMind+1 {
				notes = append(notes, fmt.Sprintf("... %d more groups", len(lists)-(heldInMind)))
				break
			}
		}

		var pic strings.Builder
		fmt.Fprintf(&pic, "    %s\n", shortPkg(pkgPath))
		fmt.Fprintf(&pic, "      shown %d (used outside)\n", len(shownFns))
		fmt.Fprintf(&pic, "      hidden %d\n", len(hiddenFns))

		file, line := "", 0
		if pkg := findPkg(prod, pkgPath); pkg != nil && pkg.Fset != nil && len(pkg.Syntax) > 0 {
			file, line = posLine(pkg.Fset, pkg.Syntax[0].Pos())
		}
		diags = append(diags, diagnostic{
			lint:    "shallow-package",
			key:     pkgPath,
			message: fmt.Sprintf("`%s` shows %d functions (≥%d) and hides only %d", shortPkg(pkgPath), len(shownFns), heldInMind, len(hiddenFns)),
			file:    file,
			line:    line,
			notes:   notes,
			picture: pic.String(),
		})
	}
	return diags
}

func productionPackages(pkgs []*packages.Package) []*packages.Package {
	var out []*packages.Package
	for _, p := range pkgs {
		if isTestPackage(p) {
			continue
		}
		if strings.HasSuffix(p.PkgPath, "/cmd") || strings.Contains(p.PkgPath, "/cmd/") {
			continue
		}
		out = append(out, p)
	}
	return out
}

func findPkg(pkgs []*packages.Package, path string) *packages.Package {
	for _, p := range pkgs {
		if p.PkgPath == path {
			return p
		}
	}
	return nil
}

func exportedFuncs(pkgs []*packages.Package) map[*types.Func]struct{} {
	out := map[*types.Func]struct{}{}
	for _, p := range pkgs {
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			obj := scope.Lookup(name)
			if !obj.Exported() {
				continue
			}
			switch o := obj.(type) {
			case *types.Func:
				out[o] = struct{}{}
			case *types.TypeName:
				named, ok := o.Type().(*types.Named)
				if !ok {
					continue
				}
				for i := range named.NumMethods() {
					m := named.Method(i)
					if m.Exported() {
						out[m] = struct{}{}
					}
				}
				ms := types.NewMethodSet(types.NewPointer(named))
				for i := range ms.Len() {
					sel := ms.At(i)
					fn, _ := sel.Obj().(*types.Func)
					if fn != nil && fn.Exported() && fn.Pkg() == p.Types {
						out[fn] = struct{}{}
					}
				}
			}
		}
	}
	return out
}

func thingName(obj types.Object) string {
	fn, ok := obj.(*types.Func)
	if !ok {
		return "funcs"
	}
	sig, _ := fn.Type().(*types.Signature)
	if sig == nil || sig.Recv() == nil {
		return "funcs"
	}
	recv := sig.Recv().Type()
	if p, ok := recv.(*types.Pointer); ok {
		recv = p.Elem()
	}
	if n, ok := recv.(*types.Named); ok {
		return n.Obj().Name()
	}
	return "funcs"
}
