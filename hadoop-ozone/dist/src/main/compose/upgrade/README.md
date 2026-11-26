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
    2. Add a new line: `run_test ha non-rolling <newly-released-version> "$OZONE_CURRENT_VERSION"` before the commented out lines.

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

The upgrade suite lives in the *upgrades/upgrade* directory.

- The upgrade steps are controlled by the *upgrades/upgrade/driver.sh* script. Callbacks to execute throughout the upgrade are called by this script and should be placed in a file called *callback.sh* in the *upgrades/upgrade/callbacks/\<upgrade-to>* directory.

- As the test is run, result logs and docker volume data for the upgrade for these versions will be placed in *upgrades/upgrade/execution/\<compose-cluster>-\<execution-mode>-\<upgrade-from>-\<upgrade-to>*. This allows a suite of upgrades to be run without conflicting directory names.

- The result logs of all upgrades run as part of the tests will be copied to a *result* directory in the top level upgrade directory.

#### Execution modes

The same driver and callbacks are used for both kinds of upgrade. The `EXECUTION_MODE` (`rolling` or `non-rolling`), passed as the second argument to `run_test`, selects how the cluster moves between versions and which robot tests in *smoketest/upgrade/\<execution-mode>* are used to finalize and check finalization status.

- `non-rolling`: Any necessary conversion of on disk structures from the old version to the new version are handled by Ozone's non-rolling upgrade framework. Before each version change the OMs are prepared, then the whole cluster (including the client) is stopped and started again with the other version.

- `rolling`: The first upgrade is done as a rolling (zero-downtime) upgrade, restarting one service at a time. The downgrade and the re-upgrade restart all servers at once, unless `DOWNGRADE_AND_REUPGRADE_ROLLING` is set to `true` in *driver.sh*. The client is upgraded after all servers, before finalization. Both the start and end version must support rolling upgrade.

#### callbacks

- The name of each subdirectory in *callbacks* is a version to upgrade to, with a *callback.sh* file whose callbacks will be invoked for any upgrade ending in that version. These versions were released before rolling upgrade support, so these callbacks only apply to the `non-rolling` mode and use the non-rolling robot tests.

- The *common* directory contains callbacks used for all upgrade tests regardless of the version and execution mode.

- Supported Callbacks:
    1. `with_old_version`: Run while ozone is in the original version to start the upgrade from, before any upgrade steps have been done.
    2. `with_this_version_pre_finalized`: Run after ozone is stopped in the old version, and brought back up and running in the new version pre-finalized.
    3. `with_old_version_downgraded`: Run after ozone is stopped in the new version pre-finalized, and restarted in the old version again.
    4. `with_this_version_finalized`: Run after ozone is stopped in the old version after donwgrade, started again in the new version pre-finalized, and then finalized.
        - The upgrade is complete when this callback runs.
    5. `with_service_stopped`, `with_service_restarted`: Only in `rolling` mode, run while a single service is stopped, and after it was started again with the target version.

### compose

Docker compose cluster definitions to be used in upgrade testing are defined in the *compose* directory. A compose cluster can be selected by specifying the name of its subdirectory as the first argument to `run_test`. `run_test` will then source the `load.sh` script in the cluster's directory so it is used during the test.

## Persisting Data

- Data for each container is persisted in a mounted volume.

- By default it's *data* under the *upgrades/upgrade/execution/\<compose-cluster>-\<execution-mode>-\<from-version>-\<to-version>* directory, but can be overridden with the `OZONE_VOLUME` environment variable.

- Mounting volumes allows data to be persisted in the cluster throughout container restarts, meaning that tests can check that data written in older versions is still readable in newer versions.

- Data will be available after the tests finish for debugging purposes. It will be erased on a following run of the test.

## Extending

### Adding New Tests

- Tests that should run for all upgrades, regardless of the version being tested, can be added to *compose/upgrade/upgrades/upgrade/callbacks/common/callback.sh*.

- Tests that should run only for an upgrade to a specific version can be added to *compose/upgrade/upgrades/upgrade/callbacks/\<ending-upgrade-version>/callback.sh*.

- Add commands in the callback function when they should be run. Each callback file will have access to the following environment variables:
  - `OZONE_UPGRADE_FROM`: The version of ozone being upgraded from.
  - `OZONE_UPGRADE_TO`: The version of ozone being upgraded to.
  - `EXECUTION_MODE`: The execution mode of the upgrade, `rolling` or `non-rolling`.
  - `TEST_DIR`: The top level *upgrade* directory containing all files for upgrade testing.
  - `SCM`: The name of the SCM container.
  - `CLIENT`: The container to run robot tests from.
    - This can be passed as the first argument to `execute_robot_test`.
    - It is the dedicated `client` container in the *ha* cluster, and the SCM container in the *non-ha* cluster.

### Testing New Versions

- To test upgrade between different versions, add a line `run_test <compose-cluster-directory> <rolling|non-rolling> <old-version> <new-version>` to the top level *test.sh* file.
    -  The `run_test` function will execute *upgrades/upgrade/driver.sh* in the given execution mode with the callbacks defined in *upgrades/upgrade/callbacks/common/callback.sh* and *upgrades/upgrade/callbacks/\<new-version>/callback.sh*.

- The variable `OZONE_CURRENT_VERSION` is used to define the version corresponding to the locally built source code in the `apache/ozone-runner` image.
    - All other versions will be treated as tags specifying a released version of the `apache/ozone` docker image to use.

