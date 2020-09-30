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

Ozone can use topology related information (for example rack placement) to optimize read and write pipelines. To get full rack-aware cluster, Ozone requires three different configuration.

 1. The topology information should be configured by Ozone.
 2. Topology related information should be used when Ozone chooses 3 different datanodes for a specific pipeline/container. (WRITE)
 3. When Ozone reads a Key it should prefer to read from the closest node. 

<div class="alert alert-warning" role="alert">

Ozone uses RAFT replication for Open containers (write), and an async replication for closed, immutable containers (cold data). As RAFT requires low-latency network, topology awareness placement is available only for closed containers. See the [page about Containers]({{< ref "concept/Containers.md">}}) about more information related to Open vs Closed containers.

</div>

## Topology hierarchy

Topology hierarchy can be configured with using `net.topology.node.switch.mapping.impl` configuration key. This configuration should define an implementation of the `org.apache.hadoop.net.CachedDNSToSwitchMapping`. As this is a Hadoop class, the configuration is exactly the same as the Hadoop Configuration

### Static list

Static list can be configured with the help of ```TableMapping```:

```XML
<property>
   <name>net.topology.node.switch.mapping.impl</name>
   <value>org.apache.hadoop.net.TableMapping</value>
</property>
<property>
   <name>net.topology.table.file.name</name>
   <value>/opt/hadoop/compose/ozone-topology/network-config</value>
</property>
```

The second configuration option should point to a text file. The file format is a two column text file, with columns separated by whitespace. The first column is a DNS or IP address and the second column specifies the rack where the address maps. If no entry corresponding to a host in the cluster is found, then `/default-rack` is assumed. 

### Dynamic list 

Rack information can be identified with the help of an external script:


```XML
<property>
   <name>net.topology.node.switch.mapping.impl</name>
   <value>org.apache.hadoop.net.TableMapping</value>
</property>
<property>
   <name>org.apache.hadoop.net.ScriptBasedMapping</name>
   <value>/usr/local/bin/rack.sh</value>
</property>
```

If implementing an external script, it will be specified with the `net.topology.script.file.name` parameter in the configuration files. Unlike the java class, the external topology script is not included with the Ozone distribution and is provided by the administrator. Ozone will send multiple IP addresses to ARGV when forking the topology script. The number of IP addresses sent to the topology script is controlled with `net.topology.script.number.args` and defaults to 100. If `net.topology.script.number.args` was changed to 1, a topology script would get forked for each IP submitted.

## Write path

Placement of the closed containers can be configured with `ozone.scm.container.placement.impl` configuration key. The available container placement policies can be found in the `org.apache.hdds.scm.container.placement` [package](https://github.com/apache/hadoop-ozone/tree/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/placement/algorithms). 

By default the `SCMContainerPlacementRandom` is used for topology-awareness the `SCMContainerPlacementRackAware` can be used:

```XML
<property>
   <name>ozone.scm.container.placement.impl</name>
   <value>org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware</value>
</property>
```

This placement policy complies with the algorithm used in HDFS. With default 3 replica, two replicas will be on the same rack, the third one will on a different rack.
 
This implementation applies to network topology like "/rack/node". Don't recommend to use this if the network topology has more layers.
 
## Read path

Finally the read path also should be configured to read the data from the closest pipeline.

```XML
<property>
   <name>ozone.network.topology.aware.read</name>
   <value>true</value>
</property>
```

## References

 * Hadoop documentation about `net.topology.node.switch.mapping.impl`: https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/RackAwareness.html
 * [Design doc]({{< ref "design/topology.md">}})