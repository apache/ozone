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

This directory contains a sample cluster definition and script for
testing upgrades from any previous version to another previous version, or the current version.

## Directory Layout

Each type of upgrade has a subdirectory under this top level upgrade directory. Each upgrade's steps are controlled by a *test.sh* script in its *\<upgrade-type>* directory. Callbacks to execute throughout the upgrade are called by this script and should be placed in a file called *callback.sh* in the *\<upgrade-type>/\<upgrade-from>-\<upgrade-to>* directory. After the test is run, results and docker volume data for the upgrade for these versions will also be placed in this directory. The results of all upgrades run as part of the tests will be placed in a *results* folder in the top level upgrade directory.


## Upgrade Types

### manual-upgrade

- Any necessary conversion of on disk structures from the old version to the new version must be done explicitly.

- This is primarily for testing upgrades of versions before the non-rolling upgrade framework was introduced in 1.1.0.

- Supported Callbacks:
    1. `setup_with_old_version`: Run before ozone is started in the old version.
    3. `with_old_version`: Run while ozone is running in the old version.
    3. `setup_with_new_version`: Run after ozone is stopped in the old version, but before it is restarted in the new version.
    4. `with_new_version`: Run while ozone is running in the new version.

### non-rolling-upgrade

- Any necessary conversion of on disk structures from the old version to the new version are handled by Ozone's non-rolling upgrade framework.

- Supported Callbacks:
    1. `setup`: Run before ozone is started in the old version.
    3. `with_old_version`: Run while ozone is running in the old version.
    3. `with_new_version_pre_finalized`: Run after ozone is stopped in the old version, and brought back up and running in the new version pre finalized.
    4. `with_old_version_downgraded`: Run after ozone is stopped in the new version pre finalized, and restarted in the old version again.
        - This is how a user would do a downgrade of an upgrade instead of finalizing to finish the upgrade.
    5. `with_new_version_finalized`: Run after ozone is stopped in the old version after donwgrade, started again in the new version pre finalized, and then finalized.
        - The upgrade will be complete by this point.

- Note that on the first upgrade after the non-rolling upgrade fraemwork is added, the old version does not have the non-rolling upgrade framework, but the new version does.
    - The non-rolling upgrade framework can still be used, the only difference is that OMs cannot be prepared before moving from the old version to the new version.
    - Set the variable `OZONE_PREPARE_OMS` to `false` disable OM preparation as part of the upgrade.

## Persisting Data

- Data for each container is persisted in a mounted volume.

- By default it's `data` under the *compose/upgrade/\<versions>* directory, but can be overridden with the `OZONE_VOLUME` environment variable.

- This allows data to be persisted in the cluster throughout container restarts, meaning that tests can check that data written in older versions is still readable in newer versions.

- Data will be available after the tests finish for debugging purposes. It will be erased on a following run of the test.

## Extending

### Adding New Tests

- To add tests to an existing upgrade type, go into its *compose/upgrade/\<upgrade-type>/\<versions>/callback.sh* file and add commands in the callback function when they should be run.

- Each callback file will have access to the following environment variables:
    - `OZONE_UPGRADE_FROM`: The version of ozone being upgraded from.
    - `OZONE_UPGRADE_TO`: The version of ozone being upgraded to.
    - `COMPOSE_DIR`: The top level directory containing the files *docker-compose.yaml* and *testlib.sh*.

### Testing New Versions

- To test upgrade between different versions, add a line `run_test <upgrade-type> <old-version> <new-version>` to the top level *test.sh* file.
    -  The `run_test` function will execute *\<upgrade-type>/test.sh* with the callbacks defined in *\<upgrade-type>/\<old-version>-\<new-version>/callback.sh*.

- If one of the specified versions does not match the current version defined by `OZONE_CURRENT_VERSION`, it will be pulled from the corresponding *apache/ozone* docker image.
    - Else, the current version will be used, which will run the locally built source code in the `apache/ozone-runner` image.
