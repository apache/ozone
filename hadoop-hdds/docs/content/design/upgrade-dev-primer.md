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
When upgrading a component from an older version to a newer version which has a higher layout version, the component automatically goes into a pre-finalized state after which an explicit ‘finalize’ action is required from the user to finalize it. In the pre-finalized state, commands/APIs/on disk structures used and created by newer layout features are meant to be unsupported or unused. After finalizing, the newer layout feature APIs are supported.

## Downgrade
Downgrade to a lower version is allowed from the pre-finalized state. This involves stopping the component, replacing the artifacts to the older version, and then starting it up again.

# Useful framework tools to use

## LayoutFeature
    org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature
    org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature
Class to add  a new layout feature being brought in. Layout version is typically 1 + last layout feature in that catalog. 

## LayoutVersionManager
    org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager
    org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager
Every component carries an instance of this interface, which provides APIs to get runtime layout version, and if a feature is allowed based on that or not.

The LayoutVersionManager interface carries an API that can be used to check if a feature is allowed in the current layout version.
     org.apache.hadoop.ozone.upgrade.LayoutVersionManager#isAllowed(org.apache.hadoop.ozone.upgrade.LayoutFeature)

## @DisallowedUntilLayoutVersion Annotation
Method level annotation used to "disallow" an API if current layout version does not include the associated layout feature. Currently it is added only to the OM module, but can easily be moved down to a common module based on need on the HDDS layer.

## @BelongsToLayoutVersion Annotation
Annotation to mark an OM request class that it belongs to a specific Layout Version. Until that version is available post finalize, this request will not be supported. A newer version of an existing OM request can be created (by inheritance or a fully new class) and marked with a newer layout version. Until finalizing this layout version, the older request class is used. Post finalizing, the newer version of the request class is used.

## LayoutVersionInstanceFactory<T>
Generic factory which stores different instances of Type 'T' sharded by a key & version. A single key can be associated with different versions of 'T'.

### Why does this class exist?
A typical use case during upgrade is to have multiple versions of a class / method / object and choose them based  on the current layout version at runtime. Before finalizing, an older version is typically needed, and after finalization, a newer version is needed. This class serves this purpose in a generic way. For example, we can create a Factory to create multiple versions of OMRequests sharded by Request Type & Layout Version Supported.

## Upgrade Action (UpgradeActionOm & UpgradeActionHdds)
Annotation to specify upgrade action run during specific upgrade phases. Each layout feature can optionally define an upgrade action for every supported phase. These are the supported phases of action callbacks.

#### VALIDATE_IN_PREFINALIZE
A ‘validation’ action run every time a component is started up with this layout feature being unfinalized.

- Example: Stopping a component if a new configuration is used prior to it being finalized.

- Example: Cleaning up from a failed ON_FINALIZE action that may have left on disk data in an inoperable state.
    - Note that because the ON_FINALIZE action failed, the feature remains pre-finalized.

#### ON_FIRST_UPGRADE_START
A backward compatible action run once when an upgraded cluster is detected with this new layout version. This differs from VALIDATE_IN_PREFINALIZE because it will not be run again once it completes successfully.
This action will be run again if it fails partway through, and may be run again if another error occurs during the upgrade. The action must always leave on disk data in a backwards compatible state, even if it fails partway through, since it is being executed before finalization.

- Example: The new version expects data in a different location even when it is pre-finalized. The action creates a symlink in the new location pointing to the old location.

#### ON_FINALIZE
An action run once during finalization of layout version (feature). This action will be run again if it fails partway through, and may be run again if another error occurs during the upgrade. This is the only action permitted to make backwards incompatible changes to on disk structures, since finalization has been initiated by the time it is run. If a failure partway through could leave the component in an inoperable state, a cleanup action should be used in VALIDATE_IN_PREFINALIZE, which will be run when the component is restarted after a failure.

- Example: Adding a new RocksDB column family.

- Example: Logging a message saying a feature is being finalized.

## ‘Prepare’ the Ozone Manager
Used to flush all transactions to disk, take a DB snapshot, and purge the logs, leaving Ratis in a clean state without unapplied log entries. This prepares the OM for upgrades/downgrades so that no request in the log is applied to the database in the old version of the code in one OM, and the new version of the code in another OM.

To prepare an OM quorum, run
 
    ozone admin om -id=<om-sevice-id> prepare

To cancel preparation of an OM quorum, run
 
    ozone admin om -id=<om-sevice-id> cancelprepare


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
