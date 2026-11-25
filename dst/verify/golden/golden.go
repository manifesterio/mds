// Package golden checks campaign fingerprints against fixed expected hashes.
package golden

import (
	"bufio"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// Assert fails t when want[name] is missing or differs from hash.
func Assert(t testing.TB, name string, hash uint64, want map[string]uint64) {
	t.Helper()
	exp, ok := want[name]
	if !ok {
		t.Fatalf("golden %q: no expected hash", name)
	}
	if exp != hash {
		t.Fatalf("golden %q: got %#x want %#x", name, hash, exp)
	}
}

// AssertCampaign fails t when got and want differ for any seed.
// Missing or extra seeds are reported with clear seed lists.
func AssertCampaign(t testing.TB, got, want map[uint64]uint64) {
	t.Helper()
	var missing, extra, mismatch []string
	for seed, exp := range want {
		h, ok := got[seed]
		if !ok {
			missing = append(missing, strconv.FormatUint(seed, 10))
			continue
		}
		if h != exp {
			mismatch = append(mismatch, fmt.Sprintf("seed %d: got %#x want %#x", seed, h, exp))
		}
	}
	for seed := range got {
		if _, ok := want[seed]; !ok {
			extra = append(extra, strconv.FormatUint(seed, 10))
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	slices.Sort(mismatch)
	var b strings.Builder
	if len(missing) > 0 {
		fmt.Fprintf(&b, "missing seeds [%s]; ", strings.Join(missing, ", "))
	}
	if len(extra) > 0 {
		fmt.Fprintf(&b, "extra seeds [%s]; ", strings.Join(extra, ", "))
	}
	if len(mismatch) > 0 {
		fmt.Fprintf(&b, "mismatches: %s", strings.Join(mismatch, "; "))
	}
	if b.Len() > 0 {
		t.Fatalf("golden campaign: %s", strings.TrimSpace(b.String()))
	}
}

// Load reads a golden file of lines "seed hash" (decimal seed, hex or decimal hash).
// Blank lines and lines starting with # are ignored.
func Load(path string) (map[uint64]uint64, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is the caller's golden file by design
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	out := map[uint64]uint64{}
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		seedStr, hashStr, ok := strings.Cut(line, " ")
		if !ok {
			return nil, fmt.Errorf("%s:%d: want \"seed hash\"", path, lineNo)
		}
		seed, err := strconv.ParseUint(seedStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: seed: %w", path, lineNo, err)
		}
		hash, err := strconv.ParseUint(hashStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: hash: %w", path, lineNo, err)
		}
		out[seed] = hash
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Save writes got as sorted "seed 0xhash" lines.
func Save(path string, got map[uint64]uint64) error {
	seeds := slices.Sorted(maps.Keys(got))

	var b strings.Builder
	b.WriteString("# seed hash\n")
	for _, seed := range seeds {
		fmt.Fprintf(&b, "%d %#x\n", seed, got[seed])
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

// Record is one richer golden line: scenario, seed, steps, rand draws, and hash.
// On disk: "<scenario> seed=<n> steps=<n> rand=<n> hash=<016x>".
type Record struct {
	Scenario string
	Seed     uint64
	Steps    uint64
	Rand     uint64
	Hash     uint64
}

// FormatRecord renders r in the richer golden line form.
func FormatRecord(r Record) string {
	return fmt.Sprintf("%s seed=%d steps=%d rand=%d hash=%016x", r.Scenario, r.Seed, r.Steps, r.Rand, r.Hash)
}

// ParseRecord parses one richer golden line.
func ParseRecord(line string) (Record, error) {
	line = strings.TrimSpace(line)
	scenario, rest, ok := strings.Cut(line, " ")
	if !ok || scenario == "" {
		return Record{}, errors.New("want \"<scenario> seed=... steps=... rand=... hash=...\"")
	}
	fields := map[string]uint64{}
	for part := range strings.SplitSeq(rest, " ") {
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			return Record{}, fmt.Errorf("want key=value in %q", part)
		}
		base := 10
		if key == "hash" {
			base = 16 // FormatRecord writes hash as bare %016x (no 0x prefix)
		}
		n, err := strconv.ParseUint(val, base, 64)
		if err != nil {
			return Record{}, fmt.Errorf("%s: %w", key, err)
		}
		fields[key] = n
	}
	for _, key := range []string{"seed", "steps", "rand", "hash"} {
		if _, ok := fields[key]; !ok {
			return Record{}, fmt.Errorf("missing %s", key)
		}
	}
	return Record{
		Scenario: scenario,
		Seed:     fields["seed"],
		Steps:    fields["steps"],
		Rand:     fields["rand"],
		Hash:     fields["hash"],
	}, nil
}

// LoadRecords reads richer golden lines from path. Blank lines and # comments
// are ignored. Order is file order.
func LoadRecords(path string) ([]Record, error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is the caller's golden file by design
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var out []Record
	sc := bufio.NewScanner(f)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		r, err := ParseRecord(line)
		if err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, lineNo, err)
		}
		out = append(out, r)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// SaveRecords writes records as richer golden lines (file order preserved).
func SaveRecords(path string, records []Record) error {
	var b strings.Builder
	for _, r := range records {
		b.WriteString(FormatRecord(r))
		b.WriteByte('\n')
	}
	return os.WriteFile(path, []byte(b.String()), 0o600)
}

// AssertFile compares got to the richer golden file at path. When update is
// true, the file is rewritten from got and the test passes.
func AssertFile(t testing.TB, path string, got []Record, update bool) {
	t.Helper()
	if update {
		if err := SaveRecords(path, got); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := LoadRecords(path)
	if err != nil {
		t.Fatalf("%v (create it with -update-golden)", err)
	}
	gotText := formatRecords(got)
	wantText := formatRecords(want)
	if gotText != wantText {
		t.Fatalf("trace hashes changed. If this is intended, review it and re-run with -update-golden.\nwant:\n%sgot:\n%s", wantText, gotText)
	}
}

func formatRecords(records []Record) string {
	var b strings.Builder
	for _, r := range records {
		b.WriteString(FormatRecord(r))
		b.WriteByte('\n')
	}
	return b.String()
}
