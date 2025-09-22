---
title: Storage Capacity Distribution Dashboard
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

---

# Key Features

## 1. Storage Distribution Analysis

Detailed breakdown of storage usage across components:

- **Global Used Space**: Represents the actual physical storage consumed on the DataNodes.
- **Global Namespace Space**: Logical size of the namespace, calculated as the sum of pendingDirectorySize + pendingKeySize + totalOpenKeySize + totalCommittedSize, multiplied by the replication factor.
- **Open Keys and Open Files**: Space occupied by data in open keys and files that are not yet finalized.
- **Committed Keys**: Space used by fully committed key–value pairs.
- **Component-wise Distribution**: Breakdown of metrics across OM, SCM, and DataNodes.

## 2. Deletion Progress Monitoring

Track pending deletions at various stages:

- **OM Pending Deletions**: Keys marked for deletion at OM
- **SCM Pending Deletions**: Container-level deletions managed by SCM
- **DataNode Pending Deletions**: Block-level deletion metrics on each DataNode

## 3. Cluster Overview Metrics

Summarized cluster statistics:

- Total capacity and used space
- Free space distribution across components

---

# Implementation Approaches

## Approach 1: Recon-based Implementation

Leverage the existing Recon service to build the dashboard with centralized and efficient data collection.  
Recon currently maintains synchronization with the OM database and constructs the NSSummary tree, providing established calculation logic for metrics such as openKeysBytes and committedBytes.  
Additionally, Recon already possesses a comprehensive physical and logical capacity breakdown through its OM DB insights component.

### Benefits

- **Unified Data Source**: All metrics aggregated centrally in Recon
- **Performance Optimization**: Incremental sync reduces the load
- **Reduced Overhead**: Avoids redundant calculations across services
- **Code Reusability**: Built on top of existing Recon infrastructure and endpoints

### Component-wise Enhancements

#### **DataNodes (DN)**

- **Current State**: DNs expose storage metrics in their reports
- **Enhancement**:
  - Add `pending deletion byte counters` in container metadata
  - Calculate total pending per DN from container metadata and publish metrics
- **Responsibilities**:
  - Report actual and pending deletion usage per container

#### **Storage Container Manager (SCM)**

- **Current Gap**: No block size tracking in the block deletion process
- **Enhancement**:
  - Track block sizes when OM issues a deletion request
  - Send a deletion command to DN along with block size and replication factor

  ```
  OM → SCM: block deletion request + block size  
  SCM → DN: delete command + block size + replication factor
  ```

- **Responsibilities**:
  - Serve as the metadata bridge between logical keys and physical blocks

#### **Ozone Manager (OM)**

- **Enhancement**:
  - Compute block sizes during deletion
- **Responsibilities**:
  - Expose logical storage metrics: committed keys, open keys, namespace usage

#### **Recon**

- **Enhancement**:
  - Add a new dashboard aggregating:
    - Logical metrics from OM
    - Deletion progress from SCM
    - Container-level metadata from DNs
- **Data Sources**:
  - OM DB (via Insight Sync)
  - SCM Client API
  - DN BlockDeletingService metrics

---
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
  - Block sizes associated with deletion requests and its caching in scm. 
  - This can be accessed by Recon using scmClient and can be exposed as metrics.
  - This can also be updated along with OM db syncing so that we will get proper time series data.

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

---

# Summary

The proposed dashboard improves visibility into cluster storage dynamics, providing deeper insights for effective debugging and informed decision-making.  
Recon is the ideal place to host the point-in-time overview, while exposing metrics enables Prometheus and Grafana to track long-term trends.  
Together, these complementary approaches give operators a unified and powerful view of both current cluster state and historical storage patterns.

---
