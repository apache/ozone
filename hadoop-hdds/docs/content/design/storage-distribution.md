---
title: Storage Capacity Distribution Dashboard
summary: Proposal for introducing a comprehensive storage distribution dashboard in Recon for enhanced cluster monitoring and debugging capabilities.
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
- **Committed Keys**: Space utilized by fully committed key–value pairs.
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
Additionally, Recon already possesses a comprehensive physical and logical capacity to break down information through its OM DB insights component.
These existing capabilities can be effectively leveraged to minimize development effort and ensure consistency. 
While certain enhancements to OM are required regardless of the chosen implementation approach—whether CLI-based or Prometheus-driven—the foundational data processing infrastructure is already in place.
The modifications outlined for OM, SCM, and DataNode components remain mandatory across all proposed approaches to ensure complete and accurate storage distribution reporting.

### Benefits

- **Unified Data Source**: All metrics aggregated centrally in Recon. 
- **Performance Optimization**: Incremental sync reduces a load
- **Reduced Overhead**: Avoids redundant calculations across services
- **Code Reusability**: Built on top of existing Recon infrastructure and endpoints

### Component-wise Enhancements

#### **DataNodes (DN)**

- **Current State**: DNs expose storage metrics in their reports
- **Enhancement**:
  - Add `pending deletion byte counters` in container metadata
  - Calculate total pending per DN from these container metadata and publish metrics.
- **Responsibilities**:
  - Report actual and pending deletion usage per container

#### **Storage Container Manager (SCM)**

- **Current Gap**: No block size tracking in a block deletion process
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
  - DN BlockDeletingService metrics.

---
## Approach 2: Prometheus + Grafana Implementation (Not Recommended)

### Overview

This approach would involve publishing storage distribution metrics directly from individual components (OM, SCM, DataNodes) to Prometheus, with visualization handled entirely through Grafana dashboards.

### Why This Approach Is Not Recommended

#### **1. Customer Adoption and User Experience**
- **Current Reality**: Customers are already actively using Recon for storage analysis and monitoring
- **Existing Feedback**: Users have specifically identified gaps in Recon's current calculations and requested improvements within the existing interface
- **User Workflow Disruption**: Introducing a completely separate monitoring stack would fragment the user experience
- **Training and Adoption Overhead**: Teams would need to learn new tools and workflows, creating adoption barriers

#### **2. Incomplete Current State**
The primary driver for this enhancement is that **customers have identified that Recon's existing calculations are incomplete or incorrect**. Key issues include:
- Inconsistent storage usage calculations across different views
- Missing pending deletion visibility at granular levels
- Lack of real-time correlation between logical and physical storage metrics
- Incomplete breakdown of storage distribution across cluster components

Moving to Prometheus/Grafana would not address these calculation issues. It would simply relocate them to a different platform while requiring significant additional implementation effort.

#### **3. Recon's Existing Infrastructure Advantages**
- **Data Access**: Recon already has optimized access to OM DB, SCM metadata, and DN reports
- **Calculation Engine**: Existing framework for cross-component metric aggregation and correlation
- **Web Interface**: Established a UI framework for complex data visualization and drill-down capabilities
- **User Base**: Active user community familiar with Recon's interface and capabilities

## Approach 3: CLI-based (Not Proceeding)

A CLI-based approach was evaluated to compute detailed usage and pending deletion breakdown by analyzing offline OM and SCM database checkpoints and querying DataNodes. 
While it offers precise, up-to-date results and independence from Recon, it introduces significant operational overhead.

This approach requires generating and processing large metadata snapshots, which can take hours in large-scale clusters. 
Given its complexity, dependency on manual execution, and high resource consumption, we have chosen not to proceed with the CLI-based solution and instead focus on enhancing Recon for better usability and integration.

---

# Summary

The proposed dashboard improves visibility into cluster storage dynamics, providing deeper insights for effective debugging and informed decision-making. 
Recon is the ideal place to host this feature, given its established role as the central storage overview in Ozone.

---
