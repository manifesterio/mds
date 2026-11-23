package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunReplay(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-scenario", "kafka", "-seed", "2"}, &out, &errb); code != 0 {
		t.Fatalf("code %d\n%s%s", code, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "seed 2: steps=") {
		t.Fatal(out.String())
	}
}

// TestRunKafkaIsTheDefaultScenario: no -scenario runs kafka.
func TestRunKafkaIsTheDefaultScenario(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"-seeds", "1", "-j", "1"}, &out, &errb)
	if code != 0 && code != 1 {
		t.Fatalf("code %d\n%s%s", code, out.String(), errb.String())
	}
	if !strings.HasPrefix(out.String(), "kafka ") {
		t.Fatal(out.String())
	}
}

func TestRunVariablesAndBadArgs(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"-scenario", "nope"}, &out, &errb); code != 2 || !strings.Contains(errb.String(), `unknown scenario "nope"`) {
		t.Fatal(code, errb.String())
	}
	errb.Reset()
	if code := run([]string{"-bogus"}, &out, &errb); code != 2 {
		t.Fatal(code)
	}
	if code := run([]string{"-h"}, &out, &errb); code != 0 {
		t.Fatal(code)
	}
	// The kafka-log scenarios live in the control plane repo and are not here.
	if code := run([]string{"-scenario", "kafka-log"}, &out, &errb); code != 2 {
		t.Fatal(code)
	}
}

// TestTheScenarioIsRegistered: the name is part of the command line of the tool.
func TestTheScenarioIsRegistered(t *testing.T) {
	names := registry().Names()
	if len(names) != 1 || names[0] != "kafka" {
		t.Fatalf("scenarios %v", names)
	}
}
