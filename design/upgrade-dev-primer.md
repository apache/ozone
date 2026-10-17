---
title: Upgrade - Developer Primer 
summary: Helpful resources for those who are bringing layout changes.
date: 2021-02-15
author: Aravindan Vijayan 
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

# Terminologies

## Layout Feature
A layout feature is any new Ozone feature that makes a backward incompatible change to the on disk layout. Each layout feature is associated with a layout version that it defines. A component has a list of monotonically increasing layout features (versions) that it supports. 

## Finalizing & Pre-Finalized State
When upgrading a component from an older version to a newer version which has a higher layout version, the component automatically goes into a pre-finalized state after which an explicit 'finalize' action is required from the user to finalize it. In the pre-finalized state, commands/APIs/on disk structures used and created by newer layout features are meant to be unsupported or unused. After finalizing, the newer layout feature APIs are supported.

## Downgrade
Downgrade to a lower version is allowed from the pre-finalized state. This involves stopping the component, replacing the artifacts to the older version, and then starting it up again.

# Useful framework tools to use

## LayoutFeature
    org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature
    org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature
Class to add  a new layout feature being brought in. Layout version is typically 1 + last layout feature in that catalog. 

## Version management (OM vs HDDS)

**Ozone Manager** uses [`org.apache.hadoop.ozone.om.upgrade.OMVersionManager`](https://github.com/apache/ozone/blob/master/hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMVersionManager.java) ([`org.apache.hadoop.ozone.upgrade.ComponentVersionManager`](https://github.com/apache/ozone/blob/master/hadoop-hdds/framework/src/main/java/org/apache/hadoop/ozone/upgrade/ComponentVersionManager.java)), with upgrade actions discovered via [`org.apache.hadoop.ozone.om.upgrade.OMUpgradeActionProvider`](https://github.com/apache/ozone/blob/master/hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/upgrade/OMUpgradeActionProvider.java). It exposes apparent/software `ComponentVersion` and `isAllowed(ComponentVersion)` for gating.

**SCM** uses [`org.apache.hadoop.hdds.scm.server.upgrade.ScmVersionManager`](https://github.com/apache/ozone/blob/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/server/upgrade/ScmVersionManager.java) ([`org.apache.hadoop.ozone.upgrade.ComponentVersionManager`](https://github.com/apache/ozone/blob/master/hadoop-hdds/framework/src/main/java/org/apache/hadoop/ozone/upgrade/ComponentVersionManager.java)), with upgrade actions via [`org.apache.hadoop.hdds.upgrade.ScmUpgradeActionProvider`](https://github.com/apache/ozone/blob/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/upgrade/ScmUpgradeActionProvider.java). Cluster-wide SCM finalization uses [`org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager`](https://github.com/apache/ozone/blob/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/server/upgrade/FinalizationManager.java).

**DataNode** uses [`org.apache.hadoop.ozone.container.upgrade.DatanodeVersionManager`](https://github.com/apache/ozone/blob/master/hadoop-hdds/container-service/src/main/java/org/apache/hadoop/ozone/container/upgrade/DatanodeVersionManager.java) with upgrade actions via [`org.apache.hadoop.ozone.container.upgrade.DatanodeUpgradeActionProvider`](https://github.com/apache/ozone/blob/master/hadoop-hdds/container-service/src/main/java/org/apache/hadoop/ozone/container/upgrade/DatanodeUpgradeActionProvider.java). Both SCM and DataNode expose apparent/software `ComponentVersion` and `isAllowed(ComponentVersion)` for gating (including legacy [`HDDSLayoutFeature`](https://github.com/apache/ozone/blob/master/hadoop-hdds/framework/src/main/java/org/apache/hadoop/hdds/upgrade/HDDSLayoutFeature.java) checks where still used).

**Recon** uses [`org.apache.hadoop.ozone.recon.upgrade.ReconVersionManager`](https://github.com/apache/ozone/blob/master/hadoop-ozone/recon/src/main/java/org/apache/hadoop/ozone/recon/upgrade/ReconVersionManager.java) for Derby SQL schema versioning, with versions defined in [`ReconVersion`](https://github.com/apache/ozone/blob/master/hadoop-ozone/recon/src/main/java/org/apache/hadoop/ozone/recon/upgrade/ReconVersion.java) and upgrade actions via [`ReconUpgradeActionProvider`](https://github.com/apache/ozone/blob/master/hadoop-ozone/recon/src/main/java/org/apache/hadoop/ozone/recon/upgrade/ReconUpgradeActionProvider.java). Apparent version is stored in `RECON_SCHEMA_VERSION`. Recon also runs [`ScmVersionManager`](https://github.com/apache/ozone/blob/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/server/upgrade/ScmVersionManager.java) for SCM-lite metadata and finalizes both tracks on startup.

## @DisallowedUntilLayoutVersion Annotation
Method level annotation used to "disallow" an API if current layout version does not include the associated layout feature. Currently it is added only to the OM module, but can easily be moved down to a common module based on need on the HDDS layer.

## @BelongsToLayoutVersion Annotation
Annotation to mark an OM request class that it belongs to a specific Layout Version. Until that version is available post finalize, this request will not be supported. A newer version of an existing OM request can be created (by inheritance or a fully new class) and marked with a newer layout version. Until finalizing this layout version, the older request class is used. Post finalizing, the newer version of the request class is used.

## Upgrade Action (UpgradeActionOm, UpgradeActionScm, UpgradeActionDatanode & UpgradeActionRecon)
Annotation to specify upgrade action run during finalization. Each layout feature can optionally define a single upgrade action that will be executed when the feature is finalized. This action should be idempotent and execute quickly. The action must complete for the feature to finish
finalizing, so if there is an error executing the action it will be retried. This partial failure should not leave the component inoperable.

- Example: Adding a new RocksDB column family.

- Example: Logging a message saying a feature is being finalized.

## 'Prepare' the Ozone Manager
Used to flush all transactions to disk, take a DB snapshot, and purge the logs, leaving Ratis in a clean state without unapplied log entries. This prepares the OM for upgrades/downgrades so that no request in the log is applied to the database in the old version of the code in one OM, and the new version of the code in another OM.

To prepare an OM quorum, run

    ozone admin om prepare -id=<om-sevice-id>

To cancel preparation of an OM quorum, run

    ozone admin om cancelprepare -id=<om-service-id>


## When do you bring in a change as a Layout feature?
By using the new feature, if it creates a change in disk layout (RocksDB, HDDS Volume etc) that is incompatible with the older version, then that qualifies as a layout feature.

#### What are some examples of changes that need not be layout features?
- A feature that creates a table and writes into it. On downgrade, the table
is no longer accessed (as expected), but does not interfere with the existing
 functionality.
- A change to an existing OM request that does not change on disk layout.  

## Testing layout feature onboarding
- Ability to deploy Mini ozone cluster with any layout version
    - org.apache.hadoop.ozone.MiniOzoneCluster.Builder#setScmLayoutVersion
    - org.apache.hadoop.ozone.MiniOzoneCluster.Builder#setOmLayoutVersion
    - org.apache.hadoop.ozone.MiniOzoneCluster.Builder#setDnLayoutVersion 
- Acceptance test framework provides callbacks at various upgrade stages
 (older, pre-finalize, on downgrade, on finalize) to plug in data ingestion / verification logic. 
    - Please refer to hadoop-ozone/dist/src/main/compose/upgrade/README.md
