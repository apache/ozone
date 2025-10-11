<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Ozone Upgrade Acceptance Tests

This directory contains cluster definitions and scripts for testing upgrades from any previous version to another
previous version, or to the local build of the code. It is designed to catch backwards incompatible changes made between
an older release of Ozone and a later release (which may be the local build).

## Quick Guide For Release Managers

- The whole test matrix of upgrading and downgrading to/from previous releases to the current code is too time consuming to do on every CI run. Instead we recommend release managers manually test the full matrix before each release, and let only the tests from the previous release to the current code be run in CI.

1. Before the release, test the whole matrix of upgrades from the previous version to the version you are releasing.
    - This is important to manually verify that the release does not break backwards compatibility. The results can be included in the release vote mailing thread.
    - To do this, uncomment all lines that contain `run_test` in the *test.sh* file, and execute *test.sh* either locally or on GitHub actions.

2. After the release is finished and its docker image is published, add the new version to the test matrix.
    1. Comment out all `run_test` lines in *test.sh*.
    2. Add a new line: `run_test ha non-rolling-upgrade <newly-released-version> "$OZONE_CURRENT_VERSION"` before the commented out lines.

## Important Notes on Test Scope

- These tests will not catch backwards incompatible changes against commits in between releases.
    - Example:
        1. After 1.2.0, a change *c1* is made that is backwards compatible with 1.2.0.
        2. After *c1*, a new change *c2* is made that is also backwards compatible with 1.2.0 but backwards *incompatible* with *c1*.

        - This test suite will not raise an error for *c2*, because it only tests against the last release
        (1.2.0), and not the last commit (*c1*).

## Supported Versions

Non-rolling upgrades and downgrades are supported from 1.1.0 to any later version. Note that 1.1.0 did not have the non-rolling upgrade framework, so things like preparing the OMs for upgrade and checking finalization status are not present in that version.

## Directory Layout

### upgrades

Each type of upgrade has a subdirectory under the *upgrades* directory.

- Each upgrade's steps are controlled by a *driver.sh* script in its *upgrades/\<upgrade-type>* directory. Callbacks to execute throughout the upgrade are called by this script and should be placed in a file called *callback.sh* in the *upgrades/\<upgrade-type>/\<upgrade-to>* directory.

- As the test is run, result logs and docker volume data for the upgrade for these versions will be placed in *upgrades/\<upgrade-type>/execution/\<upgrade-from>-\<upgrade-to>*. This allows a suite of upgrades to be run without conflicting directory names.

- The result logs of all upgrades run as part of the tests will be copied to a *result* directory in the top level upgrade directory.

#### non-rolling-upgrade

- Any necessary conversion of on disk structures from the old version to the new version are handled by Ozone's non-rolling upgrade framework.

- The name of each subdirectory in *non-rolling-upgrade* is a version to start the upgrade test from, with a *callback.sh* file whose callbacks will be invoked for any upgrade starting in that version.

- The *common* directory contains callbacks used for all upgrade tests regardless of the version.

- Supported Callbacks:
    1. `with_old_version`: Run while ozone is in the original version to start the upgrade from, before any upgrade steps have been done.
    2. `with_this_version_pre_finalized`: Run after ozone is stopped in the old version, and brought back up and running in the new version pre-finalized.
    3. `with_old_version_downgraded`: Run after ozone is stopped in the new version pre-finalized, and restarted in the old version again.
    4. `with_this_version_finalized`: Run after ozone is stopped in the old version after donwgrade, started again in the new version pre-finalized, and then finalized.
        - The upgrade is complete when this callback runs.

### compose

Docker compose cluster definitions to be used in upgrade testing are defined in the *compose* directory. A compose cluster can be selected by specifying the name of its subdirectory as the first argument to `run_test`. `run_test` will then source the `load.sh` script in the cluster's directory so it is used during the test.

## Persisting Data

- Data for each container is persisted in a mounted volume.

- By default it's *data* under the *upgrades/\<upgrade-type>/execution/\<from-version>-\<to-version>* directory, but can be overridden with the `OZONE_VOLUME` environment variable.

- Mounting volumes allows data to be persisted in the cluster throughout container restarts, meaning that tests can check that data written in older versions is still readable in newer versions.

- Data will be available after the tests finish for debugging purposes. It will be erased on a following run of the test.

## Extending

### Adding New Tests

- Tests that should run for all upgrades, regardless of the version being tested, can be added to *compose/upgrade/\<upgrade-type>/common/callback.sh*.

- Tests that should run only for an upgrade to a specific version can be added to *compose/upgrade/\<upgrade-type>/\<ending-upgrade-version>/callback.sh*.

- Add commands in the callback function when they should be run. Each callback file will have access to the following environment variables:
  - `OZONE_UPGRADE_FROM`: The version of ozone being upgraded from.
  - `OZONE_UPGRADE_TO`: The version of ozone being upgraded to.
  - `TEST_DIR`: The top level *upgrade* directory containing all files for upgrade testing.
  - `SCM`: The name of the SCM container to run robot tests from.
    - This can be passed as the first argument to `execute_robot_test`.
    - This allows the same tests to work with and without SCM HA.

### Testing New Versions

- To test upgrade between different versions, add a line `run_test <compose-cluster-directory> <upgrade-type> <old-version> <new-version>` to the top level *test.sh* file.
    -  The `run_test` function will execute *upgrades/\<upgrade-type>/driver.sh* with the callbacks defined in *upgrades/\<upgrade-type>/common/callback.sh* and *upgrades/\<upgrade-type>/\<new-version>/callback.sh*.

- The variable `OZONE_CURRENT_VERSION` is used to define the version corresponding to the locally built source code in the `apache/ozone-runner` image.
    - All other versions will be treated as tags specifying a released version of the `apache/ozone` docker image to use.

