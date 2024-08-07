# Talos Certifier Release Process

## Overview

The release process is split into two stages. The first stage is manual, it is done by developer, following the second stage where the rest of the release process is automated.

Developer starts the process by invoking a release script on the command line which will collect some release-related information from the developer. After script finish executing all project libraries will have their version modified in the relevant metadata files, those changes will be commited to the GIT and tagged. Al these changes are done on the developer machine and are *not affecting* the remote repository. The next logical step is to review the changes and push them to the remote.

The second stage of the release process driven by GitHub Actions where the publishing workflow gets triggered by the arrival of a new GIT tag matching a semversion format. After workflow finish executing all artefacts will be published to their respective registries and become available for external clients to consume.

As explained above, here is what overall release process does:

- produces and publishes multiple artefacts,
- changes the content of some project files,
- produces some commits and tag in the GIT tree.

Let's review these in more details, starting with artefacts.

## Artefacts made by release process

The list below is formatted in no particular order.

`@kindredgroup/cohort_sdk_client` - The Cohort SDK library for Talos Certifier clients.<br>
Published to GitHub Packages as npm package.<br>
Home: `/cohort_sdk_client`

`@kindredgroup/cohort_sdk_js` - The bridge between native `Rust` client and `JavaScript`.<br>
Published to GitHub Packages as npm package.<br>
Home: `/packages/cohort_sdk_js`


The following packages are published to [https://crates.io](https://crates.io) as Rust libraries. Their sources are under `/packages/` directory.

`talos_metrics` - Re-usable metric data structures.
<br>`talos_suffix` - Suffix implementation.
<br>`talos_certifier` - The implementation of certification logic.
<br>`talos_common_utils` - Re-usable code.
<br>`talos_rdkafka_utils` - Kafka-related utilities.
<br>`talos_certifier_adapters` - Adapters library allowing us to abstract the specific implementations of database and messaging.
<br>`talos_messenger_core` - The implementation of talos-messager.
<br>`talos_messenger_actions` - The implementation of messenger's actions.

### The dependencies between artefacts

From the user perspective, to launch any of Talos application the separate project is required which implements "main" method and uses desired Talos libraries. If the separate project is Rust project, then only Talos Rust libraries are required. However, when the separate application is NodeJS then Talos node npm libraries are required which in turn require Talos Rust libraries compiled for the specific machine's architecture on which we are intended to run NodeJS application.

Here is how it looks.

![Client Dependencies](docs/release/client.png)

## Release Procedure

### Manual Stage

The release script is located in `/scripts/release.sh`

Below is the usage example. Let's release version 1.0.0

```bash
cd talos-certifier-git/

scripts/release.sh

Fetching master from remote...
Already up to date.
Comparing current master with remote...
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

CURRENT_BRANCH=master
LATEST_TAG=v0.2.12
LATEST_SEMVER=0.2.12

Provide a new semver number. Type: 'a' or just press 'enter' for cancel and abort here.
Otherwise type version number, for example, type '2.0.0' the tag name will be v2.0.0
```

At this point, type in `1.0.0`

```bash
You answered '1.0.0', validating ...

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Provide the next version number (aka 'dev version' or 'shapshot')
Type: 'a' or just press 'enter' for cancel and abort here.
Otherwise type the next version (witout suffix), for example, if you are releasing '2.0.0' then the next dev version will be '2.1.0'. We will add '-dev' suffix automatically
```

At this point think about what is the next version going to be. For example, lets set it to `1.0.1`

```bash
1.0.1
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
The new version for release will be       : 1.0.0
The next dev version after release will be: 1.0.1-dev
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Proceed to bumping the project version? (type 'y' for 'Yes')
```

At this point type `y`

```bash
y

Bumping versions of all crates to 1.0.0

Bumping versions of the following NPMs:
  - cohort_sdk_client         1.0.0
  - packages/cohort_sdk_js    1.0.0

v1.0.0

> version
> napi version

# Here you may the non-significant warning complaining about `napi: command not found`. This happens if nami executable is not globally installed. However, it's not a problem as all the necessary executables are going to be installed as dependencies and they do not need to be global. The Warning is generated by the line `napi version` which is not in our control and automatically executed because of us invoking "npm version". Just ignore the warning.

[master 6d4f82b] chore(npm): Release npm 1.0.0
 4 files changed, 6 insertions(+), 6 deletions(-)
   Upgrading talos_common_utils from 0.2.13-dev to 1.0.0
    Updating banking_common's dependency from 0.2.13-dev to 1.0.0
    ...

cargo check
    Blocking waiting for file lock on package cache
    Finished dev [unoptimized + debuginfo] target(s) in 0.85s
cargo fmt -- --check
cargo clippy --all-targets -- -D warnings
    Blocking waiting for file lock on build directory
    Finished dev [unoptimized + debuginfo] target(s) in 0.79s
test -e .env || cp .env.example .env
bash -c 'set -o allexport; source .env; set +o allexport; make "$RECIPE"'
rust unit tests...
cargo test
    Finished test [unoptimized + debuginfo] target(s) in 1.13s
     Running unittests src/lib.rs (target/debug/deps/banking_common-77688276fa541c86)

running 6 tests
test model::tests::models ... ok
...
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s




[master 96bbdf5] chore(crate): Release 1.0.1
 22 files changed, 121 insertions(+), 121 deletions(-)
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Tagging repostiory

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Bumping development versions to 1.0.1-dev

v1.0.1-dev

> version
> napi version

# Ignore the same NAPI warning if it happens.

master 2d94b36] chore(npm): Release v1.0.1-dev
 4 files changed, 6 insertions(+), 6 deletions(-)
   Upgrading talos_common_utils from 1.0.0 to v1.0.1-dev
    Updating banking_common's dependency from 1.0.0 to v1.0.1-dev
...

running 6 tests
...

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

[master e512a06] chore(crate): Release 1.0.1-dev
 22 files changed, 121 insertions(+), 121 deletions(-)

e512a06 (HEAD -> master) chore(crate): Release 1.0.1-dev
2d94b36 chore(npm): Release 1.0.1-dev
96bbdf5 (tag: v1.0.0) chore(crate): Release 1.0.0
6d4f82b chore(npm): Release npm 1.0.0
76e9ab0 (origin/master, origin/HEAD) fix: Typo in actions/publish.yaml

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Done. Please review all changes and manually push to remote.
(To undo changes use 'git rebase -i <sha>' and 'git tag -d <tag>')
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
```

At this point all library versions where set to 1.0.0 and then tagged with tag v1.0.0 and after all library versions were set to 1.0.1-dev, indicating it as the next release candidate.

The commit tree would look like this:

```bash
git log --oneline -4

e512a06 (HEAD -> master, origin/master, origin/HEAD) chore(crate): Release 1.0.1-dev
2d94b36 chore(npm): Release 1.0.1-dev
96bbdf5 (tag: v1.0.0) chore(crate): Release 1.0.0
6d4f82b chore(npm): Release npm 1.0.0
```
These four commits were produced by our release process.

At this point we are ready to push our changes to remote.

```bash
# this pushes all commits
git push -u origin master

# this pushes new tag and triggers the second stage of release process.
git push origin v1.0.0
```

### Automatic Publishing Stage

The publishing stage is coded in this file: `/.github/workflows/publish.yml`

It will publish JS NPMs and Rust crates.

## Installation on the JS clients

When user applications declare Talos NPMs as dependencies these NPMs will get downloaded by npm from GitHub Packages registry.
The package `@kindredgroup/cohort_sdk_js` comes with bundled Talos source code but requires native talos libraries compiled for user machine's architecture. We require that users have their Rust compilation toolkit setup. When `@kindredgroup/cohort_sdk_js` is being installed by npm, the `postinstall` hook will get triggered (see snipped of `package.json`). There is script `/packages/cohort_sdk_js/scripts/postinstall.sh` which compiles native Talos libraries and finishes the conteont of cohort_sdk_js. Please refer to ["cohort_sdk_js" readme](packages/cohort_sdk_js/README.md)

```json
  "scripts": {
    "artifacts": "napi artifacts -d .",
    "build": "napi build --platform --release",
    "build:debug": "napi build --platform",
    "postinstall": "scripts/postinstall.sh",
    "universal": "napi universal",
    "version": "napi version",
    "prepack": "scripts/bundle-talos.sh",
  },
```
