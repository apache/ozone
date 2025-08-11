---
title: Listener Ozone Manager
summary: Support for Listener Ozone Managers (OMs) in Apache Ozone.
date: 2024-02-27
jira: HDDS-11523
status: implemented
author: Janus Chow
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

# Listener Ozone Manager

## Introduction

This document describes the design and implementation of Listener Ozone Managers (OMs) in Apache Ozone. This feature introduces a new type of OM that can be added to an Ozone Manager High Availability (HA) group. Listener OMs are non-voting members of the OM HA group. They receive all log entries and stay up-to-date with the leader, but they do not participate in leader election or consensus votes.

## Problem

In an Ozone Manager HA setup, all OMs are peers and participate in the Raft consensus protocol. This means that any OM can become a leader and is involved in the write path. For read-heavy workloads, it is desirable to have OMs that can serve read requests without participating in the write path. This would reduce the load on the write path and improve read performance.

## Proposed Solution

The proposed solution is to introduce "Listener" Ozone Managers. A Listener OM is a non-voting member of the OM HA group. It is built upon the "Listener" capability in Apache Ratis.

### How it works

A Listener OM is a regular OM that is configured to be a listener. When an OM is configured as a listener, it is added to the Ratis group as a listener. This means that it will receive all log entries from the leader, but it will not participate in the leader election or consensus votes.

Listener OMs can serve read requests from clients. This allows read requests to be offloaded from the voting OMs, which can improve read performance and reduce the load on the write path.

### Configuration

A new configuration key, `ozone.om.listener.nodes`, is introduced to specify which OMs should operate as listeners. This configuration should be set on all OMs in the cluster. The value of this configuration is a comma-separated list of OM node IDs.

For example:

```
<property>
  <name>ozone.om.listener.nodes</name>
  <value>om1,om2</value>
</property>
```

In this example, `om1` and `om2` will be configured as Listener OMs.

### Impact on the system

The introduction of Listener OMs has the following impact on the system:

*   **Read Performance:** Read performance is expected to improve as read requests can be served by Listener OMs.
*   **Write Performance:** Write performance is not expected to be significantly impacted.
*   **High Availability:** The high availability of the OM service is not affected. Listener OMs do not participate in leader election, so the failure of a Listener OM will not affect the availability of the OM service.

## Implementation Details

The implementation of Listener OMs involves the following key changes:

*   **OM Bootstrap Process:** The OM bootstrap process is modified to support adding new listener nodes to the cluster.
*   **Internal Protocols:** Internal protocols and data structures are updated to track whether an OM is a listener.
*   **Error Handling:** Error handling is improved for cases like attempting to transfer leadership to a listener node.

## Testing

The Listener OM feature is tested with new unit and robot tests to verify the listener functionality.

## Future Work

The following follow-up tasks have been identified:

*   **User Documentation:** User documentation needs to be updated to include information about Listener OMs.
*   **Performance Optimization:** A potential performance optimization is to prevent clients from attempting to send requests to listeners. This can be done by updating the client to be aware of Listener OMs.
