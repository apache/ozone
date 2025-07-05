---
title: "Topology awareness"
weight: 2
menu:
   main:
      parent: Features
summary: Configuration for rack-awarness for improved read/write
---
<!---
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

Apache Ozone uses topology information (e.g., rack placement) to optimize data access and improve resilience. A fully rack-aware cluster needs:

1.  Configured network topology.
2.  Topology-aware DataNode selection for container replica placement (write path).
3.  Prioritized reads from topologically closest DataNodes (read path).

## Applicability to Container Types

Ozone's topology-aware placement strategies vary by container replication type and state:

* **RATIS Replicated Containers:** Ozone uses RAFT replication for Open containers (write), and an async replication for closed, immutable containers (cold data). Topology awareness placement is implemented for both open and closed RATIS containers, ensuring rack diversity and fault tolerance during both write and re-replication operations. See the [page about Containers](concept/Containers.md) for more information related to Open vs Closed containers.
* **Erasure Coded (EC) Containers:** EC demands topology awareness from the initial write. For an EC key, OM allocates a block group of `$d+p$` distinct DataNodes selected by SCM's `ECPipelineProvider` to ensure rack diversity and fault tolerance. This topology-aware selection is integral to the EC write path for new blocks. \[2]

## Configuring Topology Hierarchy

Ozone determines DataNode network locations (e.g., racks) using Hadoop's rack awareness, configured via `net.topology.node.switch.mapping.impl` in `ozone-site.xml`. This key specifies a `org.apache.hadoop.net.CachedDNSToSwitchMapping` implementation. \[1]

Two primary methods exist:

### 1. Static List: `TableMapping`

Maps IPs/hostnames to racks using a predefined file.

* **Configuration:** Set `net.topology.node.switch.mapping.impl` to `org.apache.hadoop.net.TableMapping` and `net.topology.table.file.name` to the mapping file's path. \[1]
    ```xml
    <property>
      <name>net.topology.node.switch.mapping.impl</name>
      <value>org.apache.hadoop.net.TableMapping</value>
    </property>
    <property>
      <name>net.topology.table.file.name</name>
      <value>/etc/ozone/topology.map</value>
    </property>
    ```
* **File Format:** A two-column text file (IP/hostname, rack path per line). Unlisted nodes go to `/default-rack`. \[1]
  Example `topology.map`:
    ```
    192.168.1.100 /rack1
    datanode101.example.com /rack1
    192.168.1.102 /rack2
    datanode103.example.com /rack2
    ```

### 2. Dynamic List: `ScriptBasedMapping`

Uses an external script to resolve rack locations for IPs.

* **Configuration:** Set `net.topology.node.switch.mapping.impl` to `org.apache.hadoop.net.ScriptBasedMapping` and `net.topology.script.file.name` to the script's path. \[1]
    ```xml
    <property>
      <name>net.topology.node.switch.mapping.impl</name>
      <value>org.apache.hadoop.net.ScriptBasedMapping</value>
    </property>
    <property>
      <name>net.topology.script.file.name</name>
      <value>/etc/ozone/determine_rack.sh</value>
    </property>
    ```
* **Script:** Admin-provided, executable script. Ozone passes IPs (up to `net.topology.script.number.args`, default 100) as arguments; script outputs rack paths (one per line).
  Example `determine_rack.sh`:
    ```bash
    #!/bin/bash
    # This is a simplified example. A real script might query a CMDB or use other logic.
    while [ $# -gt 0 ] ; do
      nodeAddress=$1
      if [[ "$nodeAddress" == "192.168.1.100" || "$nodeAddress" == "datanode101.example.com" ]]; then
        echo "/rack1"
      elif [[ "$nodeAddress" == "192.168.1.102" || "$nodeAddress" == "datanode103.example.com" ]]; then
        echo "/rack2"
      else
        echo "/default-rack"
      fi
      shift
    done
    ```
  Ensure the script is executable (`chmod +x /etc/ozone/determine_rack.sh`).

  **Note:** For production environments, implement robust error handling and validation in your script. This should include handling network timeouts, invalid inputs, CMDB query failures, and logging errors appropriately. The example above is simplified for illustration purposes only.

**Topology Mapping Best Practices:**

* **Accuracy:** Mappings must be accurate and current.
* **Static Mapping:** Simpler for small, stable clusters; requires manual updates.
* **Dynamic Mapping:** Flexible for large/dynamic clusters. Script performance, correctness, and reliability are vital; ensure it's idempotent and handles batch lookups efficiently.

## Pipeline Choosing Policies

Ozone supports several policies for selecting a pipeline when placing containers. The policy for Ratis containers is configured by the property `hdds.scm.pipeline.choose.policy.impl` for SCM. The policy for EC (Erasure Coded) containers is configured by the property `hdds.scm.ec.pipeline.choose.policy.impl`. For both, the default value is `org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy`.

These policies help optimize for different goals such as load balancing, health, or simplicity:

- **RandomPipelineChoosePolicy** (Default): Selects a pipeline at random from the available list, without considering utilization or health. This policy is simple and does not optimize for any particular metric.

- **CapacityPipelineChoosePolicy**: Picks two random pipelines and selects the one with lower utilization, favoring pipelines with more available capacity and helping to balance the load across the cluster.

- **RoundRobinPipelineChoosePolicy**: Selects pipelines in a round-robin order. This policy is mainly used for debugging and testing, ensuring even distribution but not considering health or capacity.

- **HealthyPipelineChoosePolicy**: Randomly selects pipelines but only returns a healthy one. If no healthy pipeline is found, it returns the last tried pipeline as a fallback.

These policies can be configured to suit different deployment needs and workloads.

## Container Placement Policies for Replicated (RATIS) Containers

SCM uses a pluggable policy to place additional replicas of *closed* RATIS-replicated containers. This is configured using the `ozone.scm.container.placement.impl` property in `ozone-site.xml`. Available policies are found in the `org.apache.hadoop.hdds.scm.container.placement.algorithms` package \[1, 3\].

These policies are applied when SCM needs to re-replicate containers, such as during container balancing.

### 1. `SCMContainerPlacementRackAware` (Default)

* **Function:** Distributes replicas across racks for fault tolerance (e.g., for 3 replicas, aims for at least two racks). Similar to HDFS placement. \[1]
* **Use Cases:** Production clusters needing rack-level fault tolerance.
* **Configuration:**
    ```xml
    <property>
      <name>ozone.scm.container.placement.impl</name>
      <value>org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware</value>
    </property>
    ```
* **Best Practices:** Requires accurate topology mapping.
* **Limitations:** Designed for single-layer rack topologies (e.g., `/rack/node`). Not recommended for multi-layer hierarchies (e.g., `/dc/row/rack/node`) as it may not interpret deeper levels correctly. \[1]

### 2. `SCMContainerPlacementRandom`

* **Function:** Randomly selects healthy, available DataNodes meeting basic criteria (space, no existing replica), ignoring rack topology. \[1, 4\]
* **Use Cases:** Small/dev/test clusters, or if rack fault tolerance for closed replicas isn't critical.
* **Configuration:**
    ```xml
    <property>
      <name>ozone.scm.container.placement.impl</name>
      <value>org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRandom</value>
    </property>
    ```
* **Best Practices:** Not for production needing rack failure resilience.

### 3. `SCMContainerPlacementCapacity`

* **Function:** Selects DataNodes by available capacity (favors lower disk utilization) to balance disk usage. \[5, 6\]
* **Use Cases:** Heterogeneous storage clusters or where even disk utilization is key.
* **Configuration:**
    ```xml
    <property>
      <name>ozone.scm.container.placement.impl</name>
      <value>org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity</value>
    </property>
    ```
* **Best Practices:** Prevents uneven node filling.
* **Interaction:** This container placement policy selects datanodes by randomly picking two nodes from a pool of healthy, available nodes and then choosing the one with lower utilization (more free space). This approach aims to distribute containers more evenly across the cluster over time, favoring less utilized nodes without overwhelming newly added nodes.

## Topology Awareness and Placement for Erasure Coded (EC) Containers

Erasure Coding (EC) offers storage efficiency with fault tolerance, featuring distinct, intrinsic placement requirements in its write path.

### Unique EC Placement Demands

* **Inherent Rack Fault-Tolerance:** EC aims to spread `$d+p$` (data + parity) chunks of a block group across different racks for reconstruction after rack failures (up to `$p$` racks). \[2]
* **Minimum Rack Requirements:** The EC schema (e.g., RS-6-3-1024k) dictates rack diversity. Ideally, `$d+p$` distinct racks are needed for one chunk per rack. With fewer racks, the system maximizes spread, ensuring no rack holds more chunks from a stripe than parity can recover. Aim for at least `ceil((d+p)/p)` racks, preferably `$p+1$` or more. \[2]
* **Node Selection:** For EC writes, OM and SCM's `ECPipelineProvider` allocate `$d+p$` distinct DataNodes, enforcing topology for rack diversity. \[2]

### EC Container Placement Logic (Intrinsic)

* **Rack Diversity:** `ECPipelineProvider` logic places each of the `$d+p$` chunks on unique racks if possible. Otherwise, it distributes chunks evenly, ensuring no rack holds more chunks from a stripe than parity (`$p$`) can recover.
* **Node Selection Priorities:** Maximize rack/node diversity, respect rack density limits, select healthy/capacious DataNodes, and exclude unsuitable nodes.
* **Base Topology Reliance:** EC placement depends on accurate topology from `net.topology.node.switch.mapping.impl`. \[1]

### Example: RS(6,3) EC Placement (9 chunks)

* **Ideal (9+ Racks):** 1 chunk per rack; tolerates 3 rack failures.
* **Constrained (3 Racks):** 3 chunks per rack; losing 1 rack (3 chunks) is recoverable.
* **Intermediate (5 Racks):** Chunks spread (e.g., 2 chunks on 4 racks, 1 on another); tolerates losing any `$p` (3) racks.

### EC Topology Best Practices

* **Sufficient Racks:** Provision racks for EC schema fault tolerance (at least `$p+1$`, ideally `$d+p$`). \[2]
* **Even DataNode Distribution:** Distribute DataNodes evenly across racks. \[2]
* **Network & CPU:** Ensure robust network (especially inter-rack) and DataNode CPU for EC operations. \[2]

## Optimizing Read Paths

Enable by setting `ozone.network.topology.aware.read` to `true` in `ozone-site.xml`. \[1]
```xml
<property>
  <name>ozone.network.topology.aware.read</name>
  <value>true</value>
</property>
```
This directs clients (replicated data) to read from topologically closest DataNodes, reducing latency and cross-rack traffic. Recommended with accurate topology.

## Summary of Best Practices

* **Accurate Topology:** Maintain an accurate, up-to-date topology map (static or dynamic script); this is foundational.
* **Replicated (RATIS) Containers:** For production rack fault tolerance, use `SCMContainerPlacementRackAware` (mindful of its single-layer topology limitation) or `SCMContainerPlacementCapacity` (verify rack interaction) over `SCMContainerPlacementRandom`.
* **Erasure Coded (EC) Containers:** Provision sufficient racks for your EC schema. Ensure adequate network/CPU resources and use native EC libraries.
* **Read Operations:** Enable `ozone.network.topology.aware.read` with accurate topology.
* **Monitor & Validate:** Regularly monitor placement and balance; use tools like Recon to verify topology awareness.

## References

1.  [Hadoop Documentation: Rack Awareness](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/RackAwareness.html).
2.  [Ozone Erasure Coding Design Documentation (JIRA HDDS-3816)](https://issues.apache.org/jira/browse/HDDS-3816). Refer to the Apache Ozone JIRA issue HDDS-3816 and its associated design documents for authoritative details on EC implementation and placement.
3.  [Ozone Source Code: container placement policies](https://github.com/apache/ozone/tree/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/placement/algorithms). (For implementations of pluggable placement policies).
4.  [Ozone Source Code: SCMContainerPlacementRandom.java](https://github.com/apache/ozone/blob/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/placement/algorithms/SCMContainerPlacementRandom.java).
5.  [Ozone Source Code: SCMContainerPlacementCapacity.java](https://github.com/apache/ozone/blob/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/placement/algorithms/SCMContainerPlacementCapacity.java).
6.  Apache Ozone JIRA: [HDDS-3816](https://issues.apache.org/jira/browse/HDDS-3816) - Erasure Coding in Ozone.
7.  [Erasure Coding in Apache Hadoop Ozone (Design Doc)](https://ozone.apache.org/docs/edge/design/ec.html)
