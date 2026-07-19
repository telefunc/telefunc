#!/usr/bin/env bash
# Throwaway mutation runner. Requires a CLEAN tree — it reverts the target file with `git checkout`
# at the end, which would otherwise discard uncommitted work.
set -u
R="$(cd "$(dirname "$0")" && pwd)"
LABEL="$1"; F="$2"; SCRIPT="$3"
cd "$R"
if ! git diff --quiet; then
  echo "### $LABEL"
  echo "  !! WORKING TREE DIRTY — refusing to run (this script reverts with git checkout)."
  exit 2
fi
perl -0pi -e "$SCRIPT" "$F"
if git diff --quiet "$F"; then
  echo "### $LABEL"
  echo "  !! MUTATION DID NOT APPLY — pattern did not match. Not a valid grid row."
  git checkout "$F" 2>/dev/null
  exit 1
fi
OUT=$("/c/Program Files/nodejs/node.exe" node_modules/vitest/vitest.mjs run packages/telefunc/wire-protocol --reporter=verbose 2>&1 | sed 's/\x1b\[[0-9;]*m//g')
echo "### $LABEL"
echo "$OUT" | grep -E '^ +× ' | sed 's#^ *× packages/telefunc/wire-protocol/##' | sed 's/ [0-9]*ms$//'
echo "$OUT" | grep -E '^ +Tests ' | sed 's/^ *//'
git checkout "$F" 2>/dev/null
