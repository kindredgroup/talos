#!/bin/sh

BASEDIR=$(dirname "$0")

codesign -s - -v -f --entitlements ${BASEDIR}/signer-for-profiling.plist ${BASEDIR}/../$1
