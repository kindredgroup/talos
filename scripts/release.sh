#!/bin/sh
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# This script will collect some input from user and proceed with bumping versions
# in crates and libraries, setting up tag and finally bumping versions again.
#
# The following is collected from user:
#  - new version
#  - the next version
#
# The new tag will look like this: "v${newVersion}".
#
# Changes to Cargo.toml files will be comitted.
#
# Script will not proceed if project is not clean, not on "master" branch or
# current master differs from remote master.
#
# Preprequisites:
# Before running this script make sure "cargo-release" software in installed:
# https://github.com/crate-ci/cargo-release
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

line="- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"

WORK_STATUS=$(git status --porcelain)
if [ "${WORK_STATUS}" != "" ];
then
  echo $line
  echo "Working directory must be clean. Currently it contains some changes!"
  echo $WORK_STATUS
  echo $line
  exit 1
fi

# Are we allowed to execute this script on the branch other than master?
ALLOW_BRANCH="n"
for param in $1
do
  case "$param" in
    "--allow-branch")
      ALLOW_BRANCH="yes"
      echo "Execution of this script on non-master branch is allowed"
    ;;
  esac
done
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [ "$ALLOW_BRANCH" == "n" ];
then

  if [ "${CURRENT_BRANCH}" != "master" ];
  then
    echo $line
    echo "Current branch must be master!"
    echo $CURRENT_BRANCH
    echo $line
    exit 1
  else
    # refresh master branch
    echo "Fetching master from remote..."
    git fetch
    git pull

    returnStatus=$(($?+0))
    if [ $returnStatus -ne 0 ];
    then
      exit $returnStatus
    fi

    echo "Comparing current master with remote..."
    PUSH_STATUS=$(git diff HEAD origin/master --name-status)
    if [ "${PUSH_STATUS}" != "" ];
    then
      echo "There are comitted but not pushed changes in your current branch!!!"
      exit 1
    fi
  fi  # end of when on master

fi # end of allow branch

LATEST_TAG=$(git describe --abbrev=0 --tags)
LATEST_SEMVER=${LATEST_TAG/v/}

echo $line
echo ""
echo "CURRENT_BRANCH=$CURRENT_BRANCH"
echo "LATEST_TAG=$LATEST_TAG"
echo "LATEST_SEMVER=$LATEST_SEMVER"
echo ""

echo "Provide a new semver number. Type: 'a' or just press 'enter' for cancel and abort here."
echo "Otherwise type version number, for example, type '2.0.0' the tag name will be v2.0.0"
unset ANSWER
read ANSWER
if [ "${ANSWER}" == "a" ] || [ "${ANSWER}" == "" ];
then
  echo "Your answer was '${ANSWER}', aborting"
  exit 1
fi

echo "You answered '$ANSWER', validating ..."
echo ""

# IFS='.' read -a segments <<< "$ANSWER"
# len=${#segments[@]}
# if [ $len -ne 3 ];
# then
#   echo "Invalid version format: '${ANSWER}'. We expect exactly three segments separated by dot."
#   exit 1
# fi

NEW_VERSION=$ANSWER

# echo $line
# echo "Provide the next version number (aka 'dev version' or 'shapshot')"
# echo "Type: 'a' or just press 'enter' for cancel and abort here."
# echo "Otherwise type the next version (witout suffix), for example, if you are releasing '2.0.0' then the next dev version will be '2.1.0'. We will add '-dev' suffix automatically"
# unset ANSWER
# read ANSWER
# if [ "${ANSWER}" == "a" ] || [ "${ANSWER}" == "" ];
# then
#   echo "Your answer was '${ANSWER}', aborting"
#   exit 1
# fi

# NEXT_VERSION=$(echo $ANSWER | sed 's/-dev//')-dev

echo $line
echo "The new version for release will be       : $NEW_VERSION"
# echo "The next dev version after release will be: $NEXT_VERSION"
echo $line

echo "Proceed to bumping the project version? (type 'y' for 'Yes')"
unset ANSWER
read ANSWER
if [ "${ANSWER}" != "y" ];
then
  echo "Your answer was '${ANSWER}'"
  exit 1
fi

echo """
Bumping versions of all crates to $NEW_VERSION

Bumping versions of the following NPMs:
  - cohort_sdk_client         $NEW_VERSION
  - packages/cohort_sdk_js    $NEW_VERSION
"""

currentDir=$(pwd)
cd cohort_sdk_client
npm version $NEW_VERSION
cd $currentDir
cd packages/cohort_sdk_js
npm version $NEW_VERSION
cd $currentDir
git add --all
git commit -a -m "chore(npm): Release npm $NEW_VERSION" --no-verify

# # This will update version in Cargo.toml files and in dependencies, then commit
# cargo release --workspace --no-confirm --no-tag --no-publish --no-push -x $NEW_VERSION

returnStatus=$(($?+0))

# if [ $returnStatus -ne 0 ];
# then
#   echo "Exiting with build error"
#   exit $returnStatus
# fi

echo $line
echo "Tagging repostiory"
git tag -a -m "Release ${NEW_VERSION}" "v${NEW_VERSION}"
echo ""

# echo $line
# echo "Bumping development versions to $NEXT_VERSION"
# echo ""

# currentDir=$(pwd)
# cd cohort_sdk_client
# npm version $NEXT_VERSION
# cd $currentDir
# cd packages/cohort_sdk_js
# npm version $NEXT_VERSION
# cd $currentDir
# git add --all
# git commit -a -m "chore(npm): Release $NEXT_VERSION" --no-verify

# # This will update version in Cargo.toml files and in dependencies, then commit
# cargo release --workspace --no-confirm --no-tag --no-publish --no-push -x $NEXT_VERSION

# echo ""

git log --oneline -5

echo """
$line
Done. Please review all changes and manually push to remote.
(To undo changes use 'git rebase -i <sha>' and 'git tag -d <tag>')
$line
"""
