#!/usr/bin/env bash
# CI gate for the deterministic packages of dst: no goroutines, channels, map
# ranges, package-level state, or imports outside ../ops/dst/determinism-allow.txt.
#
# Opt-out rule: every package under ./core/..., ./internal/... and ./sim/... is in the gate by
# default, so a new package is checked without editing this script. A package
# that must stay out (it uses goroutines, wall time or files on purpose) is listed
# by import path in ../ops/dst/determinism-exclude.txt, with a reason. Packages
# outside those trees that stay in the gate are listed explicitly below
# (./verify/check, ./verify/check/faulty, ./verify/history).
# The script prints the final package list before it runs the check.
set -euo pipefail
cd "$(dirname "$0")/../../../dst"

module_prefix="github.com/manifesterio/mds/dst/"
exclude_file="../ops/dst/determinism-exclude.txt"

# Entries of the exclude file: drop trailing # comments, whitespace and blank lines.
excluded="$(sed -e 's/#.*//' -e 's/[[:space:]]//g' -e '/^$/d' "$exclude_file")"

pkgs=()
imports="$(go list ./core/... ./internal/... ./sim/...)"
while IFS= read -r imp; do
  if printf '%s\n' "$excluded" | grep -qxF "$imp"; then
    continue
  fi
  pkgs+=("./${imp#"$module_prefix"}")
done <<< "$imports"
pkgs+=(./verify/check ./verify/check/faulty ./verify/history)

echo "== determinism packages (${#pkgs[@]}), excluded: $(printf '%s\n' "$excluded" | wc -l | tr -d ' ') listed in ${exclude_file}"
printf '  %s\n' "${pkgs[@]}"

echo "== determinismcheck"
go run github.com/manifesterio/mds/ops/tools/determinismcheck -allow ../ops/dst/determinism-allow.txt "${pkgs[@]}"
echo "determinism gate passed"
