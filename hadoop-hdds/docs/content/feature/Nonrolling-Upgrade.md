---
title: Non-Rolling Upgrades and Downgrades
weight: 9
menu:
   main:
      parent: Features
summary: How to do non-rolling upgrades and downgrades of Ozone
---
<!--
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

Ozone supports non-rolling upgrades and downgrades, where all components are stopped first, and then restarted with the upgraded or downgraded versions.

## Upgrade States

After upgrading components, the upgrade process is divided into two states:

1. **Pre-finalized**: When the current components are stopped and the new versions are started, they will see that the data on disk was written by a previous version of Ozone and enter a pre-finalized state. In the pre-finalized state:
    - The cluster can be downgraded at any time by stopping all components and restarting with the old versions.
    - Backwards incompatible features introduced in the new version will be disallowed by the cluster.
    - The cluster will remain fully operational with all functionality present in the old version still allowed.
    - Any data created while pre-finalized will remain readable after downgrade.

2. **Finalized**: When a finalize command is given to OM or SCM, they will enter a finalized state.  In the finalized state:
    - The cluster can no longer be downgraded.
    - All new features of the cluster introduced in the new version can be used.

### Querying finalization status

**OM**: `ozone admin om finalizationstatus`. If using OM HA, finalization status is checked for the quorum, not individual OMs.

**SCM**: `ozone admin scm finalizationstatus`. SCM will report that finalization is complete once it has finalized and is aware of enough finalized datanodes to form a write pipeline. The remaining datanodes will finalize asynchronously and be incorporated into write pipelines after informing SCM that they have finalized.

**Datanodes**: `ozone admin datanode list` will list all datanodes and their health state as seen by SCM. If SCM is finalized, then datanodes whose health state is `HEALTHY` have informed SCM that they have finalized. Datanodes whose health state is `HEALTHY_READONLY` have not yet informed SCM that they have finished finalization. `HEALTHY_READONLY` (pre-finalized) datanodes remain readable, so the cluster is operational even if some otherwise healthy datanodes have not yet finalized. `STALE` or `DEAD` datanodes will be told to finalize by SCM once they are reachable again.


## Steps to upgrade and downgrade

Starting with your current version of Ozone, complete the following steps to upgrade to a newer version of Ozone.

1. If using OM HA and currently running Ozone 1.2.0 or greater, prepare the Ozone Manager. If OM HA is not being used, this step can be skipped.
    ```
    ozone admin om prepare -id=<om-sevice-id>
    ```
    The `prepare` command will block the Ozone Managers from receiving all write requests. See [Ozone Manager Prepare For Upgrade]({{< relref "../design/omprepare.md" >}}) for more information

2.  Stop all components.

3. Replace artifacts of all components with the newer versions.

4. Start the components
    1. Start the SCM and datanodes as usual:
        ```
        ozone --daemon start scm
        ```
        ```
        ozone --daemon start datanode
        ```

    2. Start the Ozone Manager using the `--upgrade` flag to take it out of prepare mode.
        ```
        ozone --daemon start om --upgrade
        ```
        - There also exists a `--downgrade` flag which is an alias of `--upgrade`. The name used does not matter.

        - **IMPORTANT**: All OMs must be started with the `--upgrade`  or `--downgrade` flag in this step. If some of the OMs are not started with this flag by mistake, run `ozone admin om cancelprepare -id=<om-sevice-id>` to make sure all OMs leave prepare mode.

At this point, the cluster is upgraded to a pre-finalized state and fully operational. The cluster can be downgraded by repeating the above steps, but restoring the older versions of components in step 3, instead of the newer versions. To finalize the cluster to use new features, continue on with the following steps.

**Once the following steps are performed, downgrading will not be possible.**

5. Finalize SCM
    ``` 
    ozone admin scm finalizeupgrade
    ```
    At this point, SCM will tell all of the datanodes to finalize. Once SCM has finalized enough datanodes to form a write pipeline, it will return that finalization was successful. The remaining pre-finalized datanodes will be in a read-only state until they indicate to SCM that they have finalized. Write requests will be directed to finalized datanodes only.

6. Finalize OM
    ```
    ozone admin om finalizeupgrade -id=<om-service-id>
    ```

At this point, the cluster is finalized and the upgrade is complete.
