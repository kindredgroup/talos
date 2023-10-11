#!/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# This script is designed to be invoked from GitHub Actions CI only.
#
# Sets the version of NPM package as developer version by adding GIT SHA suffix.
# For example, if current version comitted in repo is 1.0.1 and current SHA is abcdxyz,
# then the final dev version will be "[package-name]@1.0.1-abcdxyz"
#
# Accepts two inputs:
#   $1 - Required. The package-name of node module
#   $2 - Optional. The parameter name under which the resulting value will be exported into
#        $GITHUB_OUTPUT
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

selectNodeName=$1
outputParamName=$2

if [ "$selectNodeName" == "" ]; then
    echo "Missing the first parameter - npm package name"
    exit 1
fi

currentVersion=$(npm version | yq .$selectNodeName)
currentSha=$(git rev-parse --short HEAD)
newVersion="$currentVersion-$currentSha"
echo "D: selectNodeName=$selectNodeName"
echo "D: currentVersion=$currentVersion"
echo "D: currentSha=$currentSha"
echo "D: newVersion=$newVersion"

echo "Bumping package version"
npm version $newVersion

if [ "$outputParamName" == "" ]; then
    echo "The generated version will not be exported"
else
    echo "D: $outputParamName=$newVersion >> GITHUB_OUTPUT"
    echo "$outputParamName=$newVersion" >> "$GITHUB_OUTPUT"
fi
