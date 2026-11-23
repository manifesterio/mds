#!/bin/bash
# usage: covone.sh <module> <pkgdir> <coverpkg>
export PATH=$HOME/go/bin:$PATH
S=/private/tmp/claude-502/-Users-arun-lakshman-workspace-manifester-mds/7d4468a2-ac7c-49b2-98df-238cfe2ff1f2/scratchpad/slim
cd /Users/arun.lakshman/workspace/manifester/mds-verify-slim/$1 || exit 1
n=$(echo $2 | tr / _)
go test -count=1 -coverpkg="$3" -coverprofile=$S/cov/$1_$n.out ./$2 >$S/cov/$1_$n.log 2>&1
echo "done $1 $2 $?"
