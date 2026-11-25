package main

import (
	"fmt"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

func checkTangle(pkgs []*packages.Package) []diagnostic {
	var diags []diagnostic
	for _, p := range productionPackages(pkgs) {
		diags = append(diags, tangleInPackage(p)...)
	}
	return diags
}

func tangleInPackage(p *packages.Package) []diagnostic {
	scope := p.Types.Scope()
	var typesList []*types.TypeName
	indexOf := map[*types.TypeName]int{}
	for _, name := range scope.Names() {
		obj, ok := scope.Lookup(name).(*types.TypeName)
		if !ok || obj.Type() == nil {
			continue
		}
		if _, ok := obj.Type().(*types.Named); !ok {
			continue
		}
		indexOf[obj] = len(typesList)
		typesList = append(typesList, obj)
	}
	if len(typesList) < 2 {
		return nil
	}

	edges := make([][]int, len(typesList))
	addEdge := func(from, to *types.TypeName) {
		if from == nil || to == nil || from == to {
			return
		}
		fi, ok1 := indexOf[from]
		ti, ok2 := indexOf[to]
		if !ok1 || !ok2 {
			return
		}
		for _, e := range edges[fi] {
			if e == ti {
				return
			}
		}
		edges[fi] = append(edges[fi], ti)
	}

	for _, tn := range typesList {
		named := tn.Type().(*types.Named)
		walkKnowsTypeNames(named.Underlying(), func(other *types.TypeName) {
			addEdge(tn, other)
		})
		forEachLocalMethod(named, p.Types, func(fn *types.Func) {
			walkKnowsTypeNames(fn.Type(), func(other *types.TypeName) {
				addEdge(tn, other)
			})
		})
	}

	addMethodBodyEdges(p, indexOf, addEdge)

	var diags []diagnostic
	for _, comp := range circles(edges) {
		members := make([]*types.TypeName, len(comp))
		for i, idx := range comp {
			members[i] = typesList[idx]
		}
		files := map[string]bool{}
		for _, m := range members {
			if m.Pos().IsValid() && p.Fset != nil {
				f, _ := posLine(p.Fset, m.Pos())
				if f != "" {
					files[f] = true
				}
			}
		}
		if len(files) <= 1 && len(members) <= heldInMind {
			continue
		}
		names := make([]string, len(members))
		for i, m := range members {
			names[i] = m.Name()
		}
		sort.Strings(names)

		steps := cycleSteps(comp, edges, typesList)
		notes := []string{fmt.Sprintf("members: %s", joinMax(names, heldInMind))}
		if len(files) > 1 {
			var fl []string
			for f := range files {
				fl = append(fl, f)
			}
			sort.Strings(fl)
			notes = append(notes, fmt.Sprintf("written across %d files: %s", len(files), joinMax(fl, heldInMind)))
		}
		for _, s := range steps {
			notes = append(notes, s)
			if len(notes) > heldInMind+2 {
				break
			}
		}

		var pic strings.Builder
		fmt.Fprintf(&pic, "    package %s, %d types tangled\n", shortPkg(p.PkgPath), len(members))
		if len(steps) > 0 {
			fmt.Fprintf(&pic, "      %s\n", strings.Join(func() []string {
				out := make([]string, len(steps))
				copy(out, steps)
				return out
			}(), "\n      "))
		}

		file, line := "", 0
		if members[0].Pos().IsValid() {
			file, line = posLine(p.Fset, members[0].Pos())
		}
		diags = append(diags, diagnostic{
			lint:    "tangle",
			key:     p.PkgPath + ":" + strings.Join(names, ","),
			message: fmt.Sprintf("`%s` has a tangle of %d types", shortPkg(p.PkgPath), len(members)),
			file:    file,
			line:    line,
			notes:   notes,
			picture: pic.String(),
		})
	}
	return diags
}

func addMethodBodyEdges(p *packages.Package, indexOf map[*types.TypeName]int, addEdge func(from, to *types.TypeName)) {
	if p.TypesInfo == nil {
		return
	}
	for node, obj := range p.TypesInfo.Defs {
		fn, ok := obj.(*types.Func)
		if !ok || node == nil {
			continue
		}
		recv := receiverTypeName(fn)
		if recv == nil {
			continue
		}
		if _, ok := indexOf[recv]; !ok {
			continue
		}
		if !fn.Pos().IsValid() || !fn.Scope().Pos().IsValid() {
			continue
		}
		scope := fn.Scope()
		if scope == nil {
			continue
		}
		start, end := scope.Pos(), scope.End()
		if !start.IsValid() || !end.IsValid() {
			continue
		}
		for id, use := range p.TypesInfo.Uses {
			if id == nil || !id.Pos().IsValid() {
				continue
			}
			if id.Pos() < start || id.Pos() >= end {
				continue
			}
			tn, ok := use.(*types.TypeName)
			if !ok {
				continue
			}
			if _, ok := indexOf[tn]; ok {
				addEdge(recv, tn)
			}
		}
	}
}

func forEachLocalMethod(named *types.Named, pkg *types.Package, yield func(*types.Func)) {
	seen := map[*types.Func]bool{}
	for i := range named.NumMethods() {
		m := named.Method(i)
		seen[m] = true
		yield(m)
	}
	ms := types.NewMethodSet(types.NewPointer(named))
	for i := range ms.Len() {
		fn, _ := ms.At(i).Obj().(*types.Func)
		if fn == nil || fn.Pkg() != pkg || seen[fn] {
			continue
		}
		yield(fn)
	}
}

func receiverTypeName(fn *types.Func) *types.TypeName {
	sig, _ := fn.Type().(*types.Signature)
	if sig == nil || sig.Recv() == nil {
		return nil
	}
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok {
		return nil
	}
	return named.Obj()
}

func walkKnowsTypeNames(t types.Type, yield func(*types.TypeName)) {
	seen := map[types.Type]bool{}
	var walk func(types.Type)
	walk = func(t types.Type) {
		if t == nil || seen[t] {
			return
		}
		seen[t] = true
		switch t := t.(type) {
		case *types.Named:
			yield(t.Obj())
		case *types.Pointer:
			walk(t.Elem())
		case *types.Slice:
			walk(t.Elem())
		case *types.Array:
			walk(t.Elem())
		case *types.Chan:
			walk(t.Elem())
		case *types.Map:
			walk(t.Key())
			walk(t.Elem())
		case *types.Struct:
			for i := range t.NumFields() {
				walk(t.Field(i).Type())
			}
		case *types.Tuple:
			for i := range t.Len() {
				walk(t.At(i).Type())
			}
		case *types.Signature:
			if t.Recv() != nil {
				walk(t.Recv().Type())
			}
			walk(t.Params())
			walk(t.Results())
		case *types.Interface:
			for i := range t.NumExplicitMethods() {
				walk(t.ExplicitMethod(i).Type())
			}
			for i := range t.NumEmbeddeds() {
				walk(t.EmbeddedType(i))
			}
		}
	}
	walk(t)
}

func cycleSteps(comp []int, edges [][]int, typesList []*types.TypeName) []string {
	in := map[int]bool{}
	for _, c := range comp {
		in[c] = true
	}
	start := comp[0]
	prev := map[int]int{}
	queue := []int{start}
	prev[start] = -1
	found := -1
	for len(queue) > 0 && found < 0 {
		n := queue[0]
		queue = queue[1:]
		for _, to := range edges[n] {
			if !in[to] {
				continue
			}
			if to == start && n != start {
				prev[to] = n
				found = n
				break
			}
			if _, seen := prev[to]; !seen {
				prev[to] = n
				queue = append(queue, to)
			}
		}
	}
	if found < 0 {
		names := make([]string, len(comp))
		for i, c := range comp {
			names[i] = typesList[c].Name()
		}
		sort.Strings(names)
		return []string{"knows: " + strings.Join(names, ", ")}
	}
	var path []int
	for c := found; c != -1; c = prev[c] {
		path = append(path, c)
		if c == start {
			break
		}
	}
	reverseInts(path)
	path = append(path, start)
	var steps []string
	for i := 0; i+1 < len(path); i++ {
		a, b := typesList[path[i]].Name(), typesList[path[i+1]].Name()
		steps = append(steps, fmt.Sprintf("%s knows %s", a, b))
	}
	return steps
}

func reverseInts(a []int) {
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
}

func joinMax(items []string, max int) string {
	if len(items) <= max {
		return strings.Join(items, ", ")
	}
	return fmt.Sprintf("%s, +%d more", strings.Join(items[:max], ", "), len(items)-max)
}
