#!/bin/bash
set -e

type rustup >/dev/null 2>&1 || { echo >&2 "rustup is not installed; aborting."; exit 1; }
type cargo >/dev/null 2>&1 || { echo >&2 "cargo is not installed; aborting."; exit 1; }
type grcov >/dev/null 2>&1 || { echo >&2 "grcov is not installed; aborting."; exit 1; }
type zip >/dev/null 2>&1 || { echo >&2 "zip is not installed; aborting."; exit 1; }

base_dir="$(dirname "$0")"
cd ${base_dir}/..

app_name=talos
export CARGO_INCREMENTAL="0"
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort"

excludes="cohort_sdk_js"

echo "Compiling $app_name"
cargo +nightly-2023-12-28 build

echo "Testing $app_name"
export LLVM_PROFILE_FILE="${app_name}-%p-%m.profraw"
cargo +nightly-2023-12-28 test --tests --workspace --exclude ${excludes} # don't run doctests

rm ccov.zip 2> /dev/null || true
zip -0 ccov.zip `find . \( -name "*.gc*" \) -print | grep -v ${excludes}`

echo "Generating HTML coverage report for $app_name"
rm -rf coverage 2> /dev/null || true
mkdir coverage
grcov ccov.zip --branch -s . --llvm --ignore-not-existing --ignore "/*" --excl-start "\\\$coverage:ignore-start" --excl-stop "\\\$coverage:ignore-end" --excl-line "(//!|///|$coverage:ignore|unreachable!())" -t html -o coverage

echo "Generating LCOV coverage report for $app_name"
rm lcov.info 2> /dev/null || true
grcov ccov.zip -s . --llvm  --ignore-not-existing --ignore "/*" --excl-start "\\\$coverage:ignore-start" --excl-stop "\\\$coverage:ignore-end" --excl-line "(//!|///|$coverage:ignore|unreachable!())" -t lcov -o lcov.info

# Clean up
rm ccov.zip

# Re-run tests with JSON output
cargo +nightly-2023-12-28 test --tests --workspace --exclude ${excludes} -- -Z unstable-options --format json --report-time > coverage/test-report.json

if [ "$1" == "--open" ]; then
  index="file://$(pwd)/${base_dir}/../coverage/index.html"

  if command -v xdg-open &> /dev/null; then
    xdg-open $index
  elif command -v open &> /dev/null; then
    open $index
  else
    echo >&2 "neither xdg-open nor open are installed"
    exit 1
  fi
fi
