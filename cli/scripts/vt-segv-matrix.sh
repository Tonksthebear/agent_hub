#!/usr/bin/env bash
# Compare Botster VT SIGSEGV regression fixtures against the trybotster Ghostty
# pin vs pure ghostty-org upstream main.
#
# Usage:
#   ./scripts/vt-segv-matrix.sh both       # pin then upstream (default)
#   ./scripts/vt-segv-matrix.sh pin
#   ./scripts/vt-segv-matrix.sh upstream
#   ./scripts/vt-segv-matrix.sh --help
#
# Always restores vendor/ghostty to the ref recorded at start (best effort).
# Requires network for upstream fetch. Uses BOTSTER_ENV=test.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PIN_REMOTE="${VT_SEGV_PIN_REMOTE:-origin}"
PIN_REF="${VT_SEGV_PIN_REF:-eb72ec61304ea256be1d86ed8fa961c84e43ecbd}"
UPSTREAM_URL="${VT_SEGV_UPSTREAM_URL:-https://github.com/ghostty-org/ghostty.git}"
UPSTREAM_REF="${VT_SEGV_UPSTREAM_REF:-main}"

FIXTURES=(
  vt_crash_min
  vt_style_pressure
  vt_crash_hyperlink_map
  vt_invalid_cup
)

export BOTSTER_ENV=test
if [ -z "${BOTSTER_ZIG:-}" ] && [ -x "$HOME/.local/share/mise/installs/zig/0.16.0/bin/zig" ]; then
  export BOTSTER_ZIG="$HOME/.local/share/mise/installs/zig/0.16.0/bin/zig"
fi

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
  echo "Env:"
  echo "  VT_SEGV_PIN_REMOTE   (default: origin — trybotster remote on vendor/ghostty)"
  echo "  VT_SEGV_PIN_REF      (default: botster-vt-segv-fix)"
  echo "  VT_SEGV_UPSTREAM_URL (default: https://github.com/ghostty-org/ghostty.git)"
  echo "  VT_SEGV_UPSTREAM_REF (default: main)"
}

MODE="${1:-both}"
case "$MODE" in
  -h|--help) usage; exit 0 ;;
  pin|upstream|both) ;;
  *) echo "unknown mode: $MODE" >&2; usage; exit 2 ;;
esac

VENDOR="$ROOT/vendor/ghostty"
if [ ! -d "$VENDOR/.git" ] && [ ! -f "$VENDOR/.git" ]; then
  echo "error: $VENDOR is not a git checkout" >&2
  exit 1
fi

RESTORE_SHA="$(git -C "$VENDOR" rev-parse HEAD)"
echo "==> will restore vendor/ghostty to $RESTORE_SHA on exit"
cleanup() {
  local ec=$?
  echo "==> restoring vendor/ghostty to $RESTORE_SHA"
  git -C "$VENDOR" checkout -q "$RESTORE_SHA" 2>/dev/null || true
  exit "$ec"
}
trap cleanup EXIT

checkout_vendor() {
  local label="$1"
  local sha="$2"
  echo ""
  echo "======== Ghostty: $label ($sha) ========"
  git -C "$VENDOR" checkout -q "$sha"
  echo "building botster against vendor..."
  cargo build -q
}

run_fixtures() {
  local label="$1"
  local bin="$ROOT/target/debug/botster"
  if [ ! -x "$bin" ]; then
    echo "error: missing $bin after cargo build" >&2
    return 1
  fi

  local fail=0
  printf '%-28s %-12s %s\n' "FIXTURE" "RESULT" "DETAIL"
  printf '%-28s %-12s %s\n' "-------" "------" "------"
  for name in "${FIXTURES[@]}"; do
    local dir="$ROOT/testdata/$name"
    if [ ! -d "$dir" ]; then
      printf '%-28s %-12s %s\n' "$name" "MISSING" "$dir"
      fail=1
      continue
    fi
    set +e
    local out
    out="$("$bin" debug vt-replay --quiet --rows 70 --cols 226 "$dir" 2>&1)"
    local rc=$?
    set -e
    local result detail
    if [ "$rc" -eq 0 ]; then
      result="GREEN"
      detail="exit 0"
    elif [ "$rc" -eq 139 ] || [ "$rc" -eq 134 ]; then
      result="RED"
      detail="exit $rc (SIGSEGV/abort class)"
      fail=1
    else
      # shell often reports signal as 128+sig
      if [ "$rc" -gt 128 ]; then
        local sig=$((rc - 128))
        if [ "$sig" -eq 11 ]; then
          result="RED"
          detail="exit $rc (signal 11)"
          fail=1
        else
          result="FAIL"
          detail="exit $rc signal=$sig"
          fail=1
        fi
      else
        result="FAIL"
        detail="exit $rc"
        fail=1
      fi
    fi
    printf '%-28s %-12s %s\n' "$name" "$result" "$detail"
  done

  echo ""
  echo "Rust library tests (same fixtures through cli/test.sh):"
  set +e
  ./test.sh --unit -- botster_vt_ -- --test-threads=1
  local trc=$?
  set -e
  if [ "$trc" -eq 0 ]; then
    echo "cli/test.sh botster_vt_*: GREEN"
  else
    echo "cli/test.sh botster_vt_*: RED (exit $trc)"
    fail=1
  fi

  if [ "$fail" -ne 0 ]; then
    echo "matrix label=$label: FAILED (expected RED on unfixed upstream)"
    return 1
  fi
  echo "matrix label=$label: all green"
  return 0
}

ensure_pin_fetchable() {
  if git -C "$VENDOR" cat-file -e "$PIN_REF^{commit}" 2>/dev/null; then
    echo "==> using local pin $PIN_REF"
    return
  fi

  # Prefer trybotster remote name if present on the submodule.
  if git -C "$VENDOR" remote get-url trybotster >/dev/null 2>&1; then
    PIN_REMOTE=trybotster
  elif git -C "$VENDOR" remote get-url origin >/dev/null 2>&1; then
    local url
    url="$(git -C "$VENDOR" remote get-url origin)"
    if [[ "$url" != *trybotster/ghostty* ]]; then
      if ! git -C "$VENDOR" remote get-url trybotster >/dev/null 2>&1; then
        git -C "$VENDOR" remote add trybotster https://github.com/trybotster/ghostty.git 2>/dev/null || true
      fi
      PIN_REMOTE=trybotster
    fi
  fi
  echo "==> fetching pin $PIN_REMOTE $PIN_REF"
  git -C "$VENDOR" fetch -q "$PIN_REMOTE" "$PIN_REF" || \
    git -C "$VENDOR" fetch -q https://github.com/trybotster/ghostty.git "$PIN_REF"
}

ensure_upstream_fetchable() {
  if ! git -C "$VENDOR" remote get-url ghostty-org >/dev/null 2>&1; then
    git -C "$VENDOR" remote add ghostty-org "$UPSTREAM_URL" 2>/dev/null || \
      git -C "$VENDOR" remote set-url ghostty-org "$UPSTREAM_URL"
  fi
  echo "==> fetching upstream ghostty-org $UPSTREAM_REF"
  git -C "$VENDOR" fetch -q ghostty-org "$UPSTREAM_REF"
}

PIN_RC=0
UP_RC=0

if [ "$MODE" = "pin" ] || [ "$MODE" = "both" ]; then
  ensure_pin_fetchable
  PIN_SHA="$(git -C "$VENDOR" rev-parse "$PIN_REF" 2>/dev/null || git -C "$VENDOR" rev-parse "$PIN_REMOTE/$PIN_REF" 2>/dev/null || git -C "$VENDOR" rev-parse "FETCH_HEAD")"
  # Prefer explicit trybotster tip if fetch left FETCH_HEAD
  if git -C "$VENDOR" rev-parse -q --verify "trybotster/$PIN_REF" >/dev/null 2>&1; then
    PIN_SHA="$(git -C "$VENDOR" rev-parse "trybotster/$PIN_REF")"
  elif git -C "$VENDOR" rev-parse -q --verify "origin/$PIN_REF" >/dev/null 2>&1; then
    case "$(git -C "$VENDOR" remote get-url origin 2>/dev/null || true)" in
      *trybotster/ghostty*) PIN_SHA="$(git -C "$VENDOR" rev-parse "origin/$PIN_REF")" ;;
    esac
  fi
  checkout_vendor "trybotster pin" "$PIN_SHA"
  if ! run_fixtures "pin"; then PIN_RC=1; fi
fi

if [ "$MODE" = "upstream" ] || [ "$MODE" = "both" ]; then
  ensure_upstream_fetchable
  UP_SHA="$(git -C "$VENDOR" rev-parse "ghostty-org/$UPSTREAM_REF")"
  checkout_vendor "upstream $UPSTREAM_REF" "$UP_SHA"
  if ! run_fixtures "upstream"; then UP_RC=1; fi
fi

echo ""
echo "======== SUMMARY ========"
echo "pin:      exit $PIN_RC (0=all green)"
echo "upstream: exit $UP_RC (0=all green; non-zero expected until upstream fixes)"
echo "fixtures: ${FIXTURES[*]}"
echo "docs:     testdata/README.md"

# For `both`: pin must be green; upstream may be red (informative).
if [ "$MODE" = "both" ]; then
  exit "$PIN_RC"
fi
if [ "$MODE" = "pin" ]; then
  exit "$PIN_RC"
fi
exit "$UP_RC"
