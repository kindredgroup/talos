#!/bin/bash
set -e

type cargo >/dev/null 2>&1 || { echo >&2 "cargo is not installed; aborting."; exit 1; }
type grcov >/dev/null 2>&1 || { echo >&2 "grcov is not installed; aborting."; exit 1; }

base_dir="$(dirname "$0")"
cd "${base_dir}/.."

app_name=talos
excludes="cohort_sdk_js"

export CARGO_INCREMENTAL="0"
export RUSTFLAGS="-C instrument-coverage"
export LLVM_PROFILE_FILE="$(pwd)/target/coverage/talos-%p-%m.profraw"

echo "Cleaning old data..."
rm -rf target/coverage coverage lcov.info 2>/dev/null || true
mkdir -p target/coverage
trap 'rm -rf target/coverage/' EXIT

echo "Compiling and Testing $app_name..."
cargo test --tests --workspace --exclude ${excludes}

echo "Generating HTML report..."
grcov ./target/coverage/ \
    --binary-path ./target/debug/ \
    -s . \
    -t html \
    --branch \
    --ignore-not-existing \
    --ignore "/*" \
    --excl-start "\$coverage:ignore-start" \
    --excl-stop "\$coverage:ignore-end" \
    -o ./coverage

echo "Generating LCOV report..."
grcov ./target/coverage/ \
    --binary-path ./target/debug/ \
    -s . \
    -t lcov \
    --branch \
    --ignore-not-existing \
    --ignore "/*" \
    --excl-start "\$coverage:ignore-start" \
    --excl-stop "\$coverage:ignore-end" \
    -o lcov.info

NIGHTLY_TOOLCHAIN=$(rustup show active-toolchain | awk '{print $1}')

echo "Generating JSON test report..."
cargo +${NIGHTLY_TOOLCHAIN} test --tests --workspace --exclude ${excludes} -- \
    -Z unstable-options --format json --report-time > coverage/test-report.json

if [ "$1" == "--open" ]; then
    index="file://$(pwd)/coverage/index.html"
    command -v xdg-open &>/dev/null && xdg-open "$index" || \
    command -v open &>/dev/null && open "$index" || \
    echo "Report generated at: $index"
fi
