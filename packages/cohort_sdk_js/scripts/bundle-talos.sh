#!/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# - Copies all talos sources into TARGET_DIR (default to dist/talos)
# - Modifies Cargo.toml workspace file to include only packages/*.
# - Builds native bindings using NAPI-RS
# - Finally, cleans up and prepares the directory to be included NPM package
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

echo "D: cd $TARGET_DIR/packages/cohort_sdk_js/"
cd $TARGET_DIR/packages/cohort_sdk_js/
pwd

echo "Building project"
npm run build

echo "Current content is:"
ls -lah

echo "D: cd $CURRENT_DIR"
cd $CURRENT_DIR
pwd

echo "D: rm -rf $TARGET_DIR/target"
rm -rf $TARGET_DIR/target

echo "Copying index files into module root: $CURRENT_DIR"
cp $TARGET_DIR/packages/cohort_sdk_js/index.*s $CURRENT_DIR

echo "Moving node library into module root: $CURRENT_DIR"
mv $TARGET_DIR/packages/cohort_sdk_js/cohort_sdk_js* $CURRENT_DIR/

echo "The final content of $CURRENT_DIR"
ls -lah
