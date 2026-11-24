// Command dstgen writes the sum-type boilerplate of dst messages: marker
// methods, frozen Tag numbers, Fold methods and StateHash, from annotations.
//
// Annotations (a line comment directly above a type, or above a type group):
//
//	//dst:sum               an interface that is a sealed sum (needs //sumtype:decl)
//	//dst:sum open          a sum whose switches need not list every case
//	//dst:cases A [B ...]   each type is a case of the sums A, B
//	//dst:payload           a message type that is not a case of a sum
//	//dst:answer T          (with //dst:cases) the case is a request answered by T
//	//dst:fold manual       (with cases or payload) keep the hand-written Fold
//	//dst:state             write StateHash over the fields of the struct
//
// Tag numbers live in dst.tags next to the code. The file is append-only:
// a number never changes and is never reused. Usage:
//
//	dstgen [-check] [-import] [-base rev] [packages]
//
// With no packages it works on ".". -check writes nothing and fails when a
// generated file or dst.tags is stale. -import reads today's hand-written
// Tag methods (and Fold methods that start with h.Uint64(N)) into dst.tags.
// -base rev also fails when dst.tags lost or renumbered a line since rev.
//
// The specification is docs/design/dst-next/ADR.md, section 6.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main without the exit, for tests.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dstgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	check := fs.Bool("check", false, "write nothing; fail when generated files or dst.tags are stale")
	importTags := fs.Bool("import", false, "read existing Tag and Fold methods into dst.tags")
	base := fs.String("base", "", "with -check: also fail when dst.tags lost or renumbered a line since this git revision")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	patterns := fs.Args()
	if len(patterns) == 0 {
		patterns = []string{"."}
	}
	res, err := generate(options{patterns: patterns, importTags: *importTags})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "dstgen:", err)
		return 1
	}
	if len(res.errs) > 0 {
		for _, e := range res.errs {
			_, _ = fmt.Fprintln(stderr, e)
		}
		return 1
	}
	if *check {
		return checkOutputs(res, *base, stdout, stderr)
	}
	if err := writeOutputs(res); err != nil {
		_, _ = fmt.Fprintln(stderr, "dstgen:", err)
		return 1
	}
	return 0
}

// checkOutputs compares every output with the file on disk.
func checkOutputs(res result, base string, stdout, stderr io.Writer) int {
	stale := staleFiles(res)
	for _, path := range stale {
		_, _ = fmt.Fprintf(stderr, "%s: stale; run go generate\n", path)
	}
	bad := len(stale) > 0
	if base != "" {
		for _, e := range checkBase(res, base) {
			_, _ = fmt.Fprintln(stderr, e)
			bad = true
		}
	}
	if bad {
		return 1
	}
	_, _ = fmt.Fprintln(stdout, "dstgen: generated files are fresh")
	return 0
}
