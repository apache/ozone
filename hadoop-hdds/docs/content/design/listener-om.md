---
title: "Listener Ozone Manager"
summary: Read-only Ozone Manager to scale out read performance.
date: 2025-08-27
jira: HDDS-11523
status: implementing
author: Janus Chow, Wei-Chiu Chuang
---
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

## Introduction

The Listener Ozone Manager (OM) is a read-only, non-voting member of the OM High Availability (HA) group. It receives all log entries from the leader and stays up-to-date, but it does not participate in leader election or consensus votes. This allows Listener OMs to serve read requests from clients, which can significantly improve read performance and reduce the load on the voting OMs.

## Why use Listener OMs?

In a standard OM HA setup, all OMs are peers and participate in the Raft consensus protocol. This means that all OMs are involved in the write path, which can become a bottleneck for read-heavy workloads. By introducing Listener OMs, you can scale out your read performance by offloading read requests to these read-only OMs.

## How it works

A Listener OM is a regular OM that is configured to be a listener. When an OM is configured as a listener, it is added to the Ratis group as a listener. This means that it will receive all log entries from the leader, but it will not participate in the leader election or consensus votes.

Clients can be configured to send read requests to Listener OMs. This allows read requests to be served by the Listener OMs, which reduces the load on the voting OMs and improves read performance.

**Note:** An OM in a listener state cannot be transitioned into a leader or follower, whether automatically, or via the `ozone admin om transfer` command.

## Best Practices

*   **Recommended Cluster Topology:** For a production environment, you can have 1 or 3 voting OMs. A setup with 2 voting OMs will not work as Ratis requires an odd number of voting members for quorum. You can add any number of Listener OMs to this cluster. For high availability, 3 voting OMs are recommended.
*   **Deploy multiple Listener OMs for high availability:** To ensure that your read requests can still be served in the event of a Listener OM failure, it is recommended to deploy multiple Listener OMs.
*   **Monitor the load on your Listener OMs:** It is important to monitor the load on your Listener OMs to ensure that they are not becoming a bottleneck.
*   **Decommissioning a Listener OM:** A Listener OM can be decommissioned. The process is the same as decommissioning a regular OM. Once the Listener OM is decommissioned, it is removed from the OM HA Ring and does not receive Ratis transactions.

## Configuration

To configure a Listener OM, you need to perform the following steps:

1.  **Configure `ozone-site.xml` for all OM roles:** Add the following properties to the `ozone-site.xml` file on all OMs in the cluster. This includes the existing voting OMs and the new Listener OMs.

    ```xml
    <property>
      <name>ozone.om.service.ids</name>
      <value>cluster1</value>
    </property>
    <property>
      <name>ozone.om.nodes.cluster1</name>
      <value>om1,om2,om3,om4,om5</value>
    </property>
    <property>
      <name>ozone.om.address.cluster1.om1</name>
      <value>host1</value>
    </property>
    <property>
      <name>ozone.om.address.cluster1.om2</name>
      <value>host2</value>
    </property>
    <property>
      <name>ozone.om.address.cluster1.om3</name>
      <value>host3</value>
    </property>
    <property>
      <name>ozone.om.address.cluster1.om4</name>
      <value>host4</value>
    </property>
    <property>
      <name>ozone.om.address.cluster1.om5</name>
      <value>host5</value>
    </property>
    <property>
      <name>ozone.om.listener.nodes</name>
      <value>om4,om5</value>
    </property>
    ```

    In this example, `om1`, `om2`, and `om3` are the voting OMs, and `om4` and `om5` are the Listener OMs.

2.  **Bootstrap the Listener OM:** Before a new Listener OM can be started, it needs to be bootstrapped. This is the same process as bootstrapping a regular OM. For more details, please refer to the [OM High Availability]({{< ref "../feature/OM-HA.md#om-bootstrap" >}}) documentation.



## Consistency Guarantees

Listener OMs provide eventual consistency for read operations. This means that there may be a short delay before the latest data is available on the Listener OMs. However, for most read-heavy workloads, this delay is acceptable.

## References

* [Ozone Manager High Availability]({{< ref "../feature/OM-HA.md" >}})
