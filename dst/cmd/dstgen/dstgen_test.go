package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the want files of testdata")

// golden compares got with testdata/<dir>/want/<name>, or rewrites it with -update.
func golden(t *testing.T, dir, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", dir, "want", name)
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o600); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path) //nolint:gosec // a testdata path built by the test
	if err != nil {
		t.Fatalf("%v (run go test -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s differs from the generated output:\n--- got\n%s\n--- want\n%s", path, got, want)
	}
}

// outputs runs dstgen on one testdata package and returns its outputs by
// base name.
func outputs(t *testing.T, dir string, importTags bool) (map[string][]byte, []string) {
	t.Helper()
	res, err := generate(options{patterns: []string{"./testdata/" + dir}, importTags: importTags})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string][]byte{}
	for path, data := range res.outputs {
		byName[filepath.Base(path)] = data
	}
	return byName, res.errs
}

// TestEveryFieldKind covers sums, cases in two sums, payloads, answers,
// manual folds, state hashes and every foldable field kind, in package and
// test files.
func TestEveryFieldKind(t *testing.T) {
	out, errs := outputs(t, "kinds", false)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors:\n%s", strings.Join(errs, "\n"))
	}
	for _, name := range []string{genFile, genTestFile, tagsFile} {
		golden(t, "kinds", name+".golden", out[name])
	}
}

// TestErrors checks that every misuse is reported with its position.
func TestErrors(t *testing.T) {
	_, errs := outputs(t, "errors", false)
	dir, err := filepath.Abs(filepath.Join("testdata", "errors"))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, e := range errs {
		b.WriteString(strings.TrimPrefix(e, dir+string(os.PathSeparator)))
		b.WriteString("\n")
	}
	golden(t, "errors", "errors.golden", []byte(b.String()))
}

// TestImportTags reads hand-written Tag methods and literal Fold tags.
func TestImportTags(t *testing.T) {
	out, errs := outputs(t, "importtags", true)
	if len(errs) > 0 {
		t.Fatalf("unexpected errors:\n%s", strings.Join(errs, "\n"))
	}
	golden(t, "importtags", tagsFile+".golden", out[tagsFile])
}

// TestCheck: a package whose generated file and dst.tags are on disk is
// fresh, and the stale generated file on disk does not feed back.
func TestCheck(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-check", "./testdata/fresh"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if code := run([]string{"-check", "./testdata/kinds"}, &stdout, &stderr); code != 1 {
		t.Fatalf("kinds has no generated files on disk: want exit 1, got %d", code)
	}
	if !strings.Contains(stderr.String(), "stale") {
		t.Errorf("want a stale report, got %q", stderr.String())
	}
}

// TestTagsAppendOnly: a new case gets the next tag of its sum; existing
// numbers stay; a deleted type is retired, not reused.
func TestTagsAppendOnly(t *testing.T) {
	tf, err := parseTags("t", []byte("1 a\n2 b\n7 gone\n"))
	if err != nil {
		t.Fatal(err)
	}
	all := []*annotated{
		{name: "a", cases: []string{"s"}},
		{name: "b", cases: []string{"s"}},
		{name: "c", cases: []string{"s"}},
	}
	tf.retireMissing(func(name string) bool { return name != "gone" })
	errs := &errorList{}
	tf.assign(all, errs)
	if len(errs.list) > 0 {
		t.Fatal(errs.list)
	}
	want := tagsHeader + "1 a\n2 b\n7 -\n3 c\n"
	if got := string(tf.bytes()); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}

func TestBadTagsFile(t *testing.T) {
	for _, in := range []string{"1 a\n1 b\n", "1 a\n2 a\n", "x a\n", "1\n", "0 a\n"} {
		if _, err := parseTags("t", []byte(in)); err == nil {
			t.Errorf("%q: want an error", in)
		}
	}
}
