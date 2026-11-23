// rmdecl removes the top-level declaration (with its doc comment) that contains file:line.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
)

func main() {
	for _, arg := range os.Args[1:] {
		i := strings.LastIndex(arg, ":")
		file, line := arg[:i], arg[i+1:]
		ln, _ := strconv.Atoi(line)
		fset := token.NewFileSet()
		src, _ := os.ReadFile(file)
		f, err := parser.ParseFile(fset, file, src, parser.ParseComments)
		if err != nil {
			fmt.Println("parse", err)
			continue
		}
		done := false
		for _, d := range f.Decls {
			start, end := d.Pos(), d.End()
			var doc *ast.CommentGroup
			switch x := d.(type) {
			case *ast.FuncDecl:
				doc = x.Doc
			case *ast.GenDecl:
				doc = x.Doc
				if x.Tok == token.IMPORT {
					continue
				}
			}
			if doc != nil {
				start = doc.Pos()
			}
			if fset.Position(d.Pos()).Line <= ln && ln <= fset.Position(end).Line {
				if g, ok := d.(*ast.GenDecl); ok && len(g.Specs) > 1 && g.Lparen.IsValid() {
					// remove only the spec containing line
					for _, sp := range g.Specs {
						if fset.Position(sp.Pos()).Line <= ln && ln <= fset.Position(sp.End()).Line {
							s, e := sp.Pos(), sp.End()
							var cg *ast.CommentGroup
							switch t := sp.(type) {
							case *ast.TypeSpec:
								cg = t.Doc
							case *ast.ValueSpec:
								cg = t.Doc
							}
							if cg != nil {
								s = cg.Pos()
							}
							so, eo := fset.Position(s).Offset, fset.Position(e).Offset
							src = append(src[:so:so], src[eo:]...)
							done = true
							break
						}
					}
					if done {
						break
					}
				}
				so, eo := fset.Position(start).Offset, fset.Position(end).Offset
				src = append(src[:so:so], src[eo:]...)
				done = true
				break
			}
		}
		if done {
			os.WriteFile(file, src, 0o644)
			fmt.Println("removed decl at", arg)
		} else {
			fmt.Println("no decl at", arg)
		}
	}
}
