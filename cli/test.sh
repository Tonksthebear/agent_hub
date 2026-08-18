#!/bin/bash
# Test script for Botster Hub
#
# IMPORTANT: Always use this script instead of running `cargo test` directly.
# This ensures BOTSTER_ENV=test is set, which prevents keyring access prompts.

set -e

# Always set test mode to prevent keyring access
export BOTSTER_ENV=test

# Vendored Ghostty pin requires Zig 0.16.0 exact. Prefer the local mise
# install when present so release/integration builds do not drift.
if [ -z "${BOTSTER_ZIG:-}" ] && [ -x "$HOME/.local/share/mise/installs/zig/0.16.0/bin/zig" ]; then
    export BOTSTER_ZIG="$HOME/.local/share/mise/installs/zig/0.16.0/bin/zig"
fi

# Parse arguments
RUN_UNIT=false
RUN_INTEGRATION=false
RUN_BUILD_CHECK=false
RUN_VT_SEGV_MATRIX=false
VT_SEGV_MATRIX_MODE=both
CARGO_ARGS=""

show_help() {
    echo "Usage: ./test.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --unit, -u       Run unit tests only (cargo test --lib)"
    echo "  --integration    Run integration tests only"
    echo "  --all, -a        Run all tests (default if no option given)"
    echo "  --check, -c      Just check compilation, don't run tests"
    echo "  --vt-segv-matrix [pin|upstream|both]"
    echo "                   Run the Ghostty VT SEGV matrix (default: both)"
    echo "  --help, -h       Show this help"
    echo ""
    echo "Any additional arguments are passed to cargo test."
    echo ""
    echo "Examples:"
    echo "  ./test.sh                    # Run all tests"
    echo "  ./test.sh --unit             # Run unit tests only"
    echo "  ./test.sh --unit -- scroll   # Run unit tests matching 'scroll'"
    echo "  ./test.sh --check            # Just check compilation"
}

# Default to running all tests if no args
if [ $# -eq 0 ]; then
    RUN_UNIT=true
    RUN_INTEGRATION=true
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --unit|-u)
            RUN_UNIT=true
            shift
            ;;
        --integration)
            RUN_INTEGRATION=true
            shift
            ;;
        --all|-a)
            RUN_UNIT=true
            RUN_INTEGRATION=true
            shift
            ;;
        --check|-c)
            RUN_BUILD_CHECK=true
            shift
            ;;
        --vt-segv-matrix)
            RUN_VT_SEGV_MATRIX=true
            shift
            if [[ "${1:-}" =~ ^(pin|upstream|both)$ ]]; then
                VT_SEGV_MATRIX_MODE="$1"
                shift
            fi
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        --)
            shift
            CARGO_ARGS="$*"
            break
            ;;
        *)
            CARGO_ARGS="$CARGO_ARGS $1"
            shift
            ;;
    esac
done

# If the caller supplied only cargo test filters/args, keep the documented
# default behavior and run the full suite with those args.
if [ "$RUN_UNIT" = false ] && [ "$RUN_INTEGRATION" = false ] && [ "$RUN_BUILD_CHECK" = false ] && [ "$RUN_VT_SEGV_MATRIX" = false ]; then
    RUN_UNIT=true
    RUN_INTEGRATION=true
fi

echo "BOTSTER_ENV=$BOTSTER_ENV (keyring access disabled)"
echo ""

# Check if cargo is installed
if ! command -v cargo &> /dev/null; then
    echo "Cargo not found. Please install Rust:"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

if [ "$RUN_BUILD_CHECK" = true ]; then
    echo "Checking compilation..."
    cargo check
    echo ""
    echo "Compilation OK"
    exit 0
fi

if [ "$RUN_VT_SEGV_MATRIX" = true ]; then
    echo "Running Ghostty VT SEGV matrix ($VT_SEGV_MATRIX_MODE)..."
    ./scripts/vt-segv-matrix.sh "$VT_SEGV_MATRIX_MODE"
    echo ""
fi

if [ "$RUN_UNIT" = true ]; then
    echo "Running unit tests..."
    cargo test --lib $CARGO_ARGS
    echo ""
fi

if [ "$RUN_INTEGRATION" = true ]; then
    echo "Building release binary (required by PTY integration tests)..."
    cargo build --release
    echo ""
    echo "Running integration tests..."
    cargo test --test '*' $CARGO_ARGS
    echo ""
fi

echo "All tests passed"
