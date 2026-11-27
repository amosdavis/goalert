#!/bin/sh
set -e

# assert build-env versions are identical
BUILD_ENV_VER=go1.26.3
for file in $(find devtools -name 'Dockerfile*'); do
  if ! grep -q "goalert/build-env" "$file"; then
    continue
  fi
  if ! grep -q "goalert/build-env:$BUILD_ENV_VER" "$file"; then
    echo "build-env version mismatch, expected $BUILD_ENV_VER"
    echo "  $file:"
    echo "  $(grep goalert/build-env "$file")"
    exit 1
  fi
done
for file in $(find devtools -name '*.yml'); do
  if ! grep -q "goalert/build-env" "$file"; then
    continue
  fi
  if ! grep -q "goalert/build-env, tag: $BUILD_ENV_VER" "$file"; then
    echo "build-env version mismatch, expected $BUILD_ENV_VER"
    echo "  $file:"
    echo "  $(grep goalert/build-env "$file")"
    exit 1
  fi
done

# golangci-lint workflow MUST match go.mod and golangci-lint.version
LINT_WF=.github/workflows/golangci-lint.yml
GO_MOD_VER=$(awk '$1=="go"{print $2}' go.mod | cut -d. -f1,2)
WF_GO_VER=$(grep "go-version:" "$LINT_WF" | awk '{print $2}' | tr -d "'\"")
if [ "$GO_MOD_VER" != "$WF_GO_VER" ]; then
  echo "go-version mismatch in $LINT_WF (expected $GO_MOD_VER from go.mod, got $WF_GO_VER)"
  exit 1
fi
LINT_VER=$(cat golangci-lint.version)
WF_LINT_VER=$(grep "^ *version:" "$LINT_WF" | awk '{print $2}' | tr -d "'\"" | sed "s/^v//")
if [ "$LINT_VER" != "$WF_LINT_VER" ]; then
  echo "golangci-lint version mismatch in $LINT_WF (expected $LINT_VER from golangci-lint.version, got $WF_LINT_VER)"
  exit 1
fi

CHANGES=$(git status -s --porcelain)

if test "$CHANGES" != ""; then
  echo "Found changes in git:"
  echo "$CHANGES"
  
  # ignore if IGNORE_CHANGES is set
  if [ -z "$IGNORE_CHANGES" ]; then
    exit 1
  fi
fi
