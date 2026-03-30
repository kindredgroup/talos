#!/bin/bash
set -e

# Basic dependency checks
type cargo >/dev/null 2>&1 || { echo >&2 "cargo is not installed; aborting."; exit 1; }
type grcov >/dev/null 2>&1 || { echo >&2 "grcov is not installed; aborting."; exit 1; }

# Navigate to project root
base_dir="$(dirname "$0")"
cd "${base_dir}/.."

app_name=talos
excludes="cohort_sdk_js"

# --- 1. SET MODERN COVERAGE FLAGS ---
export CARGO_INCREMENTAL="0"
export RUSTFLAGS="-C instrument-coverage"
# This pattern ensures each process gets its own profile file
export LLVM_PROFILE_FILE="talos-%p-%m.profraw"

echo "Cleaning old data..."
rm *.profraw 2> /dev/null || true
rm -rf coverage lcov.info 2> /dev/null || true

# --- 2. RUN TESTS ---
echo "Compiling and Testing $app_name..."
# We use 'cargo' directly so it uses the toolchain provided by the Github Action
cargo test --tests --workspace --exclude ${excludes}

# --- 3. GENERATE REPORTS VIA GRCOV ---
echo "Generating HTML report..."
grcov . \
    --binary-path ./target/debug/ \
    -s . \
    -t html \
    --branch \
    --ignore-not-existing \
    --ignore "/*" \
    --excl-start "\$coverage:ignore-start" \
    --excl-stop "\$coverage:ignore-end" \
    -o ./coverage

echo "Generating LCOV report for Codecov..."
grcov . \
    --binary-path ./target/debug/ \
    -s . \
    -t lcov \
    --branch \
    --ignore-not-existing \
    --ignore "/*" \
    --excl-start "\$coverage:ignore-start" \
    --excl-stop "\$coverage:ignore-end" \
    -o lcov.info

# --- 4. OPTIONAL: JSON TEST REPORT ---
# Only runs if we are on a nightly toolchain (required for -Z flags)
if rustc --version | grep -q "nightly"; then
    echo "Generating JSON test report..."
    cargo test --tests --workspace --exclude ${excludes} -- -Z unstable-options --format json --report-time > coverage/test-report.json
fi

# Cleanup raw profile files to keep the workspace tidy
rm *.profraw

if [ "$1" == "--open" ]; then
  index="file://$(pwd)/coverage/index.html"
  command -v xdg-open &> /dev/null && xdg-open "$index" || \
  command -v open &> /dev/null && open "$index" || \
  echo "Report generated at: $index"
fi
