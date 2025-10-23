---
title: Storage Capacity Distribution Design
summary: Proposal for introducing a comprehensive storage distribution dashboard in Recon.
date: 2025-08-05
jira: HDDS-13177
status: Under Review
author: Priyesh Karatha
---

<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
  either express or implied. See the License for the specific
  language governing permissions and limitations under the License.
-->

# Abstract
Ozone currently lacks a unified interface to monitor and analyze storage distribution across its cluster components. This makes it difficult to:

- Understand data distribution across the cluster
- Debug storage reclamation issues
- Monitor pending deletion progress
- Analyze utilization patterns
- Identify potential bottlenecks and imbalances

This proposal introduces a comprehensive **Storage Capacity Distribution Dashboard** in Recon to address these challenges.
This initiative significantly enhances operational insight, capacity planning accuracy, and system observability in large-scale Ozone deployments.

## Approach 1: Recon-based

### Architectural Overview

The dashboard operates as an extension to **Recon**, leveraging existing telemetry from:

- **Ozone Manager (OM)** - namespace-level and key-level metadata.
- **Storage Container Manager (SCM)** - container, block, and replication metrics.
- **DataNodes (DNs)** - raw storage usage and pending deletion data.

All metrics are aggregated, and exposed through a **RESTful API (/storagedistribution)** for recon dashboard.

![](storage-distribution-design.jpg)
### Data Flow Summary

- OM → Recon
  - Periodic synchronization happens between OM db to Recon.
- OM -> SCM
  - Block deletion request sent by OM to SCM carries block size and replicated size.
- SCM → Recon
  - SCM aggregates the blocks in the block deletion request into DeletedBlocksTransaction with aggregated block size and replicated size, persisted into DB.
  - SCM maintains and updates a deletion transaction summary for newly created/deleted transactions. Summary is exposed through metrics, API, and also persisted.
  - Recon gets the deletion transaction summary using SCM client whenever required.
- SCM → Datanode
  - Block deletion transaction sent by SCM to DN carries block size.
- DataNode → Recon
  - Exposes TotalPendingDeletionBytes via JMX through BlockDeletingService.
  - Recon fetches this data for node-level pending deletion tracking.
- Recon
  - Consolidates all component-level data.
  - Computes global storage, namespace usage, and pending deletion metrics.
  - Exposes results via RestFull API /storagedistribution.

### Recon API Design

The Recon API will expose a single endpoint that returns a JSON object with the following structure:

| Field              | Type             | Description                            |
|--------------------|------------------|----------------------------------------|
| globalStorage      | Object           | Overall storage capacity metrics.      |
| globalNamespace    | Object           | Overall namespace (key) usage metrics. |
| usedSpaceBreakdown | Object           | Detailed breakdown of used space.      |
| dataNodeUsage      | Array of Objects | Per-DataNode storage usage.            |

#### globalStorage Object

| Field          | Type | Description                                       |
|----------------|------|---------------------------------------------------|
| totalUsedSpace | Long | Total used space across all DataNodes in bytes.   |
| totalFreeSpace | Long | Total free space across all DataNodes in bytes.   |
| totalCapacity  | Long | Total raw capacity across all DataNodes in bytes. |

#### globalNamespace Object

| Field          | Type | Description                                             |
|----------------|------|---------------------------------------------------------|
| totalUsedSpace | Long | Total used space for namespace objects (keys) in bytes. |
| totalKeys      | Long | Total number of keys in the namespace.                  |

#### usedSpaceBreakdown Object

| Field                 | Type   | Description                                            |
|-----------------------|--------|--------------------------------------------------------|
| openKeysBytes         | Long   | Bytes currently held by open keys (not yet committed). |
| committedBytes        | Long   | Bytes committed to existing keys.                      |
| containerPreAllocated | Long   | Pre-allocated space for open containers.               |
| deletionPendingBytes  | Object | Bytes pending deletion.                                |

#### deletionPendingBytes Object

| Field   | Type   | Description                                     |
|---------|--------|-------------------------------------------------|
| total   | Long   | Total bytes pending deletion across all stages. |
| byStage | Object | Breakdown of pending deletion bytes by stage.   |

#### byStage Object

| Field | Type   | Description                                         |
|-------|--------|-----------------------------------------------------|
| DN    | Object | DataNode pending deletion metrics.                  |
| SCM   | Object | Storage Container Manager pending deletion metrics. |
| OM    | Object | Ozone Manager pending deletion metrics.             |

#### DN Object (within byStage)

| Field        | Type | Description                                   |
|--------------|------|-----------------------------------------------|
| pendingBytes | Long | Bytes pending deletion at the DataNode level. |

#### SCM Object (within byStage)

| Field        | Type | Description                              |
|--------------|------|------------------------------------------|
| pendingBytes | Long | Bytes pending deletion at the SCM level. |

#### OM Object (within byStage)

| Field                 | Type | Description                                                                              |
|-----------------------|------|------------------------------------------------------------------------------------------|
| pendingKeyBytes       | Long | Key bytes pending deletion at the Ozone Manager level.                                   |
| pendingDirectoryBytes | Long | Directory bytes pending deletion at the Ozone Manager level.                             |
| totalBytes            | Long | Total bytes pending deletion at the Ozone Manager level (includes keys and directories). |
#### dataNodeUsage Array of Objects

Each object in this array represents the usage for a single DataNode.

| Field            | Type   | Description                                    |
|------------------|--------|------------------------------------------------|
| dataNodeUuId     | String | Unique identifier for the DataNode             |
| hostName         | String | Hostname of the DataNode                       |
| capacity         | Long   | Total capacity of the DataNode in bytes.       |
| used             | Long   | Used space on the DataNode in bytes.           |
| remaining        | Long   | Remaining free space on the DataNode in bytes. |
| committed        | Long   | Bytes committed to keys on this DataNode.      |
| pendingDeletion  | Long   | Bytes pending deletion on this DataNode.       |
| minimumFreeSpace | Long   | Configured minimum free space in Bytes.        |

#### Example API Output

```json
{
  "globalStorage": {
    "totalUsedSpace": 15744356352,
    "totalFreeSpace": 3002519420928,
    "totalCapacity": 3242976054744
  },
  "globalNamespace": {
    "totalUsedSpace": 5242880000,
    "totalKeys": 10
  },
  "usedSpaceBreakdown": {
    "openKeysBytes": 0,
    "committedBytes": 5242880000,
    "containerPreAllocated": 0,
    "deletionPendingBytes": {
      "total": 0,
      "byStage": {
        "DN": {
          "pendingBytes": 0
        },
        "SCM": {
          "pendingBytes": 0
        },
        "OM": {
          "pendingKeyBytes": 0,
          "totalBytes": 0,
          "pendingDirectoryBytes": 0
        }
      }
    }
  },
  "dataNodeUsage": [
    {
      "datanodeUuid": "31300d56-f6f8-46f4-9d1e-862ac82066f8",
      "hostName": "ozone-datanode-2.ozone_default",
      "capacity": 1080992018248,
      "used": 5248118784,
      "remaining": 1000839806976,
      "committed": 0,
      "pendingDeletion": 0,
      "minimumFreeSpace": 1080992000
    },
    {
      "datanodeUuid": "d211a430-6363-4882-a4b5-5d3275652e5a",
      "hostName": "ozone-datanode-3.ozone_default",
      "capacity": 1080992018248,
      "used": 5248118784,
      "remaining": 1000839806976,
      "committed": 0,
      "pendingDeletion": 0,
      "minimumFreeSpace": 1080992000
    },
    {
      "datanodeUuid": "70fd6c8c-b3f3-43ad-83ac-4e0dc6e9d74d",
      "hostName": "ozone-datanode-1.ozone_default",
      "capacity": 1080992018248,
      "used": 5248118784,
      "remaining": 1000839806976,
      "committed": 0,
      "pendingDeletion": 0,
      "minimumFreeSpace": 1080992000
    }
  ]
}
```

### Backend Implementation

The backend implementation will involve collecting data from various Ozone components, and also we need to add few code changes to get pending deletion and replicated sizes in different components.

#### Ozone Manager (OM)

Ozone Manager will be the key data source for most of the data. But SCM doesn't have the data and we need to include block size information in Deleted Block and pass it to SCM

Previously, the key block deletion requests sent from the Ozone Manager (OM) to the Storage Container Manager (SCM) only contained BlockIDs, lacking any size information. Consequently, the SCM was unable to accurately monitor the total space designated for cleanup and pending reclamation.

The new implementation will include the block size and replicated size in the key block deletion requests sent out by OM to SCM, so that SCM will know the size of each block under request to delete.

#### Storage Container Manager (SCM)

##### Persist Block Size in Delete Transactions (SCM)

SCM currently tracks pending deleted blocks by transaction metadata but lacks reliable, aggregated visibility into **how much space** (raw and replicated) those blocks occupy while they await deletion. This limits operational insights (metrics/CLI) and complicates capacity-related decisions. The design introduces block size persistence at the transaction level and an SCM-side summary to expose accurate, real-time totals.

##### Goals

- Persist **total raw size** and **total replicated size** for each DeletedBlocksTransaction.[](https://github.com/apache/ozone/pull/8845)
- Maintain an SCM-side **DeletedBlocksTransactionSummary** with totals for: transaction count, block count, total size, and total replicated size.
- Expose the summary via **metrics**, **API** and **CLI** for operators.
- Provide a safe **upgrade path** so legacy transactions do not pollute the new counters.

#### Data Node (DNs)

To enhance storage visibility, a new metric will be introduced: the total bytes consumed by blocks pending deletion. Previously, only the count of blocks awaiting deletion was tracked, providing an incomplete understanding of reclaimable disk space. This change aims to offer a more accurate and comprehensive view of storage in the process of being freed.

The core idea involves:

- **Measuring Deleted Data Size:** The system will now measure the total size in bytes of deleted items, rather than just counting them.
- **Integrating into Existing Systems:** This new size measurement will be incorporated into the current data deletion service and container data structures.
- **Calculating Size During Deletion:** Mechanisms will be implemented to accurately capture the byte size of blocks as they are marked for deletion and it will be exposed via metrics.

DataNodes currently provide storage reports, which Recon consumes regularly. However, these reports lack information about pending deletion bytes. To address this, we propose publishing TotalPendingBytes as a metric via BlockDeletingService, which Recon can then consume via JMX. This approach is more reliable and performs better than alternative solutions, such as including pending deletion information in the storage report. 

#### Recon

A new REST API endpoint, `/storagedistribution`, has been introduced to provide a consolidated view of storage distribution across the cluster. This endpoint offers various storage-related insights, including used capacity, pending deletions, and overall data distribution within Ozone components.**Key Functionalities**

Recon exposes consolidated metrics regarding storage usage and distribution, drawing data from the following sources:

- **OMDBInsightEndpoint:** Retrieves Ozone Manager (OM)-level data, such as bucket-level usage, namespace size, and used capacity.
- **SCMClient:** Fetches pending deletion metrics from the Storage Container Manager (SCM). This includes block-level metadata like size and replicated size, crucial for accurate estimation of reclaimable space.
- **Datanode Storage Reports and Metrics:** Gathers current storage usage statistics from individual datanodes. This information helps determine used versus available capacity and facilitates the classification of data distribution based on node-wise storage usage.

The pending deletion bytes are calculated and updated within the BlockDeletionService metrics, which are then exposed via JMX. This allows direct access to these metrics by monitoring tools. While this approach introduces a dependency on JMX (a minor concern in some deployments), its primary advantage is avoiding unnecessary data transfer to SCM and the associated processing overhead in the Datanode when iterating and calculating pending deletions from the container data list, as would be required with the Storage Report approach.

### New HDDS Layout and Upgrade

A new HDDS layout version, DATA_DISTRIBUTION, has been introduced to handle the potential issues during upgrading.

- In the OM to SCM block deletion request, a new field is added to protobuf to represent the block with its size. If a new OM connects to an old SCM, the request will be accepted by SCM, but the new field will be ignored by the old SCM. So OM will think these blocks are deleted by actually not, leading to orphan blocks residual in containers.
- In SCM, for an existing Ozone cluster, SCM may already have many DeletedBlocksTransactions in DB without block size information. As SCM maintains a DeletedBlocksTransactionSummary which requires update on new transactions creation, and finished transactions creation. For those old existing transactions, DeletedBlocksTransactionSummary doesn't need to be updated when they are finished and deleted. The DATA_DISTRIBUTION feature finalization action is a good timing to distinguish which are new transactions, which are old transactions.

Before DATA_DISTRIBUTION is not finalized,

- The OM to SCM block deletion request will use the current existing field for block ID info
- SCM will not collect DeletedBlocksTransactionSummary, nor it will expose the information
- Datanode will not expose the TotalPendingBytes
- Since Recon doesn't be covered in current Ozone upgrading framework, Recon should be prepared for the case that both SCM and Datanode doesn't have the info required for /storagedistribution

After the upgrade:

- The OM to SCM block deletion request will use the new field for block ID info and size
- SCM will know the start ID of new transactions, aggregate block size in OM request into DeletedBlocksTransaction, update and expose DeletedBlocksTransactionSummary. Old requests from old OM are still supported.
- Datanode will receive DeletedBlocksTransactions with or without block size included from SCM, for new transactions or old existing transactions. Datanode should handle them properly, and begin to publish metrics, including pending deletion bytes for new transactions.

#### Known Limitations
For an existing Ozone cluster updating, if there are existing pending deletion transactions in SCM and DN, these transactions will not be covered in the new data/metrics exposed by SCM and DN. So the data shown by Recon UI can vary a lot from the real total pending deletion size. The gap will reduce gradually after existing old transactions are executed and finished.

## Approach 2: CLI-based (Not Proceeding)

A CLI-based approach was evaluated to compute detailed usage and pending deletion breakdown by analyzing offline OM and SCM database checkpoints and querying DataNodes.
While it offers precise, up-to-date results and independence from Recon, it introduces significant operational overhead.

This approach requires generating and processing large metadata snapshots, which can take hours in large-scale clusters.
Given its complexity, dependency on manual execution, and high resource consumption, we have chosen not to proceed with the CLI-based solution and instead focus on enhancing Recon for better usability and integration.

## Metrics Exposure and Time-Series Tracking

While Recon provides a point-in-time view of pending deletions and storage distribution, it is equally important to track these metrics over time to understand trends and validate reclamation progress.  
To enable this, components should expose metrics that can be scraped by Prometheus and visualized in Grafana.

### Component-wise Metrics

- **Ozone Manager (OM)**
  - Recon has already methods that are available to calculate the following information.
    - open key used space
    - committed key used space 
    - containerPreAllocated space
    - pending deletion 
  - In every OM db syncing, we can update these metrics values.

- **Storage Container Manager (SCM)**
  - The DeletedBlockLogStateManager is enhanced to aggregate these block sizes in-memory, providing a DeletedBlocksTransactionSummary that includes total transaction count, total block count, total block size, and total replicated block size for pending deletions. 
  - This summary is rebuilt from persisted transaction data upon SCM startup or leader election.
  - New metrics are introduced in ScmBlockDeletingServiceMetrics to expose the aggregated deletion statistics (transaction count, block count, total size, total replicated size). This provides better operational visibility into the SCM's block deletion queue.

- **DataNodes (DN)**
  - Pending deletion bytes per node. This can be exposed as metrics in BlockDeletingService of DataNode
  - Pending deletion count per node. This can be exposed as metrics in BlockDeletingService of DataNode

### Dashboard

- **Recon UI** → Point-in-time snapshot for immediate debugging
- **Grafana** → Time-series visualization of trends such as:
  - Pending deletion backlog reduction
  - Growth of open key space
  - Utilization vs reclamation rates over time

By combining both, operators gain:
- **Real-time visibility** (via Recon)
- **Historical context** (via Grafana)

# Summary

The proposed dashboard improves visibility into cluster storage dynamics, providing deeper insights for effective debugging and informed decision-making.  
Recon is the ideal place to host the point-in-time overview, while exposing metrics enables Prometheus and Grafana to track long-term trends.  
Together, these complementary approaches give operators a unified and powerful view of both current cluster state and historical storage patterns.
