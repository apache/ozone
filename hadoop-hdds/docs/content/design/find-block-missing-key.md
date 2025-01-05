---
title: Find Block Missing Key
summary: A client-side tool used to locate Block missing Keys
date: 2024-12-09
jira: HDDS-11891
status: draft
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

## Summary

This document provides a design overview of a client-side tool used to locate Block Missing Keys.

In Ozone, the metadata of a Key is stored in the OM (Ozone Manager), while the data is stored on DNs (DataNodes). If the metadata of a Key is missing, the Key becomes invisible. However, if the metadata is intact but the data is missing, this issue cannot be identified until the Key is accessed. The goal of this tool is to detect such scenarios where the data is missing but the metadata remains intact, referred to as a Block Missing Key.


## Definitions

- Key: In this document, a Key refers to both OBJECT_STORE objects and files in FSO buckets. At the Block level, they are treated the same way.
- Missing: Refers to a situation where a Key cannot be read successfully. If a Key is missing some replicas but can still be read, it is not considered a missing Key. This tool also provides functionality to detect Keys with partially missing replicas.
- Block Missing Key: Refers to a Key whose data is missing while its metadata remains intact.


## Limitations

The following cases are not included in the scope of Block Missing Key detection in this document:

1. Key data loss caused by Block data corruption.
2. Data block integrity errors, such as mismatched chunksum values.

This tool only verifies the existence of Block data and metadata. It does not validate the correctness of data blocks, meaning it does not read Blocks to calculate their chunksum for comparison with metadata.


## Potential Scenarios for Block Missing Keys

1. The Container containing the Key is in a `DELETED` or `DELETING` state.
2. The Container containing the Key is missing, and there is no record of the Container in the cluster.
3. The metadata of the Key’s Block is missing.
4. The data of the Key’s Block is missing.


## Scanning Process Overview

1. Retrieve Key metadata:
    - Gather metadata such as the Key name and BlockID (consisting of ContainerID and localID).
    - After collecting sufficient Key metadata, organize it for further processing.
    - There are three approaches to retrieve metadata (detailed in [Three Approaches to Retrieve Metadata](https://www.notion.so/meeting-room-Conference-Room-d17916fda32244f2b5edfec93c165cee?pvs=21)).
2. Organize Keys by Container:
    - Group Keys by their respective Containers in local memory. Keys within the same Container are grouped together, and relevant information is recorded.
3. Query Container Status:
    - Send requests to the SCM (Storage Container Manager) to obtain Container locations (i.e., which DNs the Container resides on).
    - If the Container is in a `DELETED` or `DELETING` state, or if the Container cannot be found, the Key is marked as a Block Missing Key.
    - If the Container is found and its status is normal, continue to check the existence of the Blocks.
    - Containers in the `OPEN` and `RECOVERING` state should be skipped. However, for Containers in the `CLOSING` or `QUASI_CLOSED` state, further investigation is needed, as these states may indicate Block issues.
4. Query Block Existence:
    - Based on the information provided by the SCM, send `headBlocks` requests to the DNs to verify the existence of the Blocks.
    - A single `headBlocks` request can query multiple Blocks at once.
    - A new DN API for `headBlocks` requests needs to be developed.
5. Determine Block Missing Keys:
    - Once the existence of all Block replicas for a Key is verified, the client determines whether the Key is a Block Missing Key based on its replication tolerance:
        - For simple replication: the total number of replicas.
        - For EC Keys: Parity Cell count + 1.
6. Final Verification:
    - Perform an additional verification for detected Block Missing Keys to confirm whether the Key has been deleted.
    - If the Key is no longer present in OM, it is confirmed as a Block Missing Key.


## Additional Features

1. Result Output: Supports exporting results to a specified file in a predefined format.
2. Progress Tracking: Supports saving scan progress to enable resumption from checkpoints.
3. Partial Replica Check: Can detect Keys with partially missing replicas. This feature may overlap with the `find-missing-padding` command.
4. Parallel Bucket Scanning: Supports scanning multiple buckets concurrently.
5. Retry Mechanism: For large clusters, where scans may take significant time, and DN restarts are likely, the tool should support retries to improve reliability.


## Three Approaches to Retrieve Metadata

1. Using `listStatusLight` and `listKeysLight`:
    - Advantages: Lightweight and high performance.
    - Disadvantages: Requires API enhancements to include Block information.
2. Using `listStatus` and `listKeys`:
    - Advantages: Can be used directly without additional development.
    - Disadvantages: Lower performance and may become a bottleneck.
3. Directly Scanning RocksDB:
    - Advantages: Offers the best performance. If an independent snapshot is used, it can operate without affecting online services.
    - Disadvantages: Most complex implementation, and it can only be executed on nodes with access to OM RocksDB data.


## Suggestions for Improvement

1. API Enhancement: Develop lightweight APIs to directly retrieve Key Block information.
2. Fault Tolerance: Ensure robust handling of DN failures, network timeouts, and other issues to complete scans effectively.
3. Tool Integration: Consider integrating this tool into existing administration tools, such as the `ozone fs` command, for better usability.