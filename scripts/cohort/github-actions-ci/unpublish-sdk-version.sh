#!/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# This script is designed to be invoked from GitHub Actions CI only.
#
# Unpublishes given NPM from GitHub Packages NPM repository. Since simple npm unpublish
# is not supported by GitHub Packages we use "DELETE ../versions/$id" function 
# from GitHub REST API
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

echo Executing "$0"

packageName=$1
versionName=$2

if [ "$packageName" == "" ]; then
    echo "Missing the first parameter - package name to be unpublished"
    exit 1
fi

if [ "$versionName" == "" ]; then
    echo "Missing the second parameter - version name to be unpublished"
    exit 1
fi

unpublishVersion="$packageName@$versionName"

echo "Current directory:"
pwd
ls -l
echo "Current version is"
npm version
echo "Unpublishing faulty version: $unpublishVersion"
allVersionsArray=$(curl -L -H "Accept: application/vnd.github+json" -H "Authorization: Bearer $NODE_AUTH_TOKEN" -H "X-GitHub-Api-Version: $API_VERSION" "$API_URL/$packageName/versions")
echo $allVersionsArray > ./tmp_all_versions.json
lookupPackageVersionId=$(echo "$allVersionsArray" | jq --arg versionName "$versionName" '.[] | select(.name == $versionName) | .id')
if [ "$lookupPackageVersionId" == "" ];
then                
    echo "E: Unable to find package version id"
    echo "D: allVersionsArray="
    cat ./tmp_all_versions.json

    # echo ""
    # echo "D: echo allVersionsArray | yq .[]"
    # echo "$allVersionsArray" | jq '.[]'
    # echo ""
    # echo "D: versionName=$versionName"
    # echo "D: echo allVersionsArray | yq .[] | select(.name == versionName)"
    # echo "$allVersionsArray" | jq --arg versionName "$versionName" '.[] | select(.name == $versionName)'
    # echo ""
    # echo "D: full jq command result"
    # echo "$allVersionsArray" | jq --arg versionName "$versionName" '.[] | select(.name == $versionName) | .id'

    rm ./tmp_all_versions.json
    echo ""
    echo "D: curl command used:"
    echo "D: curl -L -H 'Accept: application/vnd.github+json' -H 'Authorization: Bearer ...' -H 'X-GitHub-Api-Version: $API_VERSION' $API_URL/$packageName/versions"
else
    echo "Deleting $lookupPackageVersionId of $unpublishVersion"
    echo "D: curl -L -X DELETE -H 'Accept: application/vnd.github+json' -H 'Authorization: Bearer ...' -H 'X-GitHub-Api-Version: $API_VERSION' $API_URL/$packageName/versions/$lookupPackageVersionId"
    curl -L -X DELETE -H "Accept: application/vnd.github+json" -H "Authorization: Bearer $NODE_AUTH_TOKEN" -H "X-GitHub-Api-Version: $API_VERSION" "$API_URL/$packageName/versions/$lookupPackageVersionId"
fi