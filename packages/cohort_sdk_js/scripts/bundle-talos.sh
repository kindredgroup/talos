#!/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - Copies all talos sources into TARGET_DIR (default to dist/talos)
# - Modifies Cargo.toml workspace file to include only packages/*.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

CURRENT_DIR=$(pwd)
TARGET_DIR=dist/talos
rm -r $TARGET_DIR || true
mkdir -p $TARGET_DIR/packages/cohort_sdk_js

cp -r \
    ../talos_agent \
    ../talos_certifier \
    ../talos_certifier_adapters \
    ../talos_cohort_replicator \
    ../talos_common_utils \
    ../talos_rdkafka_utils \
    ../talos_suffix \
    ../cohort_sdk \
    ../logger \
    ../metrics \
    $TARGET_DIR/packages/

cp -r \
    Cargo.toml \
    build.rs \
    package.json \
    package-lock.json \
    src \
    $TARGET_DIR/packages/cohort_sdk_js/

cp \
    ../../Cargo.toml \
    ../../rust-toolchain.toml \
    $TARGET_DIR/

cat ../../Cargo.toml \
    | grep -v "examples" \
    > $TARGET_DIR/Cargo.toml

cd $TARGET_DIR/packages/cohort_sdk_js/

echo "Building project"
npm run build

echo "Current content is:"
ls -lah

echo "D: rm -rf $CURRENT_DIR/$TARGET_DIR/target"
rm -rf $CURRENT_DIR/$TARGET_DIR/target

cd $CURRENT_DIR

echo "Copying index files into module root"
cp \
    $TARGET_DIR/packages/cohort_sdk_js/index.*s \
    $CURRENT_DIR

echo "Moving node library into module root"
mv \
    $TARGET_DIR/packages/cohort_sdk_js/cohort_sdk_js* \
    $CURRENT_DIR/

echo "Current content of $CURRENT_DIR is:"
ls -lah
