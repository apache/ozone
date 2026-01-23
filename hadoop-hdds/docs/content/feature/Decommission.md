---
title: "Decommissioning"
weight: 1
menu:
   main:
      parent: Features
summary: Decommissioning of SCM, OM and Datanode.
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

# DataNode Decommission

The DataNode decommission is the process that removes the existing DataNode from the Ozone cluster while ensuring that the new data should not be written to the decommissioned DataNode. When you initiate the process of decommissioning a DataNode, Ozone automatically ensures that all the storage containers on that DataNode have an additional copy created on another DataNode before the decommission completes. So, datanode will keep running after it has been decommissioned and may be used for reads, but not for writes until it is stopped manually.

When we initiate the process of decommissioning, first we check the current state of the node, ideally it should be "IN_SERVICE", then we change it's state to "DECOMMISSIONING" and start the process of decommissioning, it goes through a workflow where the following happens:

1. First an event is fired to close any pipelines on the node, which will also close any containers.

2. Next the containers on the node are obtained and checked to see if new replicas are needed. If so, the new replicas are scheduled.

3. After scheduling replication, the node remains pending until replication has completed.

4. At this stage the node will complete the decommission process and the state of the node will be changed to "DECOMMISSIONED".

To check the current state of the datanodes we can execute the following command,
```shell
ozone admin datanode list
```


To decommission a datanode you can execute the following command in cli,

```shell
ozone admin datanode decommission [-hV] [-id=<scmServiceId>]
       [--scm=<scm>] [<hosts>...]
```
You can enter multiple hosts to decommission multiple datanodes together.

To view the status of a decommissioning datanode, you can execute the following command:

```shell
ozone admin datanode status decommission [-hV] [-id=<scmServiceId>] [--scm=<scm>] [--id=<uuid>] [--ip=<ipAddress>]
```
You can pass the IP address or UUID of one datanode to view only the details related to that datanode.


**Note:** To recommission a datanode you may execute the below command in cli,
```shell
ozone admin datanode recommission [-hV] [-id=<scmServiceId>]
       [--scm=<scm>] [<hosts>...]
```

### Tuning and Monitoring Decommissioning

The process of decommissioning a DataNode involves replicating all its containers to other DataNodes in the cluster. The speed of this process can be tuned, and its progress can be monitored using several configuration properties and metrics.

#### Configuration Properties

Administrators can adjust the following properties in `ozone-site.xml` to control the container replication speed during decommissioning. They are grouped by the component where they are primarily configured.

##### SCM-Side Properties

*   **`hdds.scm.replication.datanode.replication.limit`**
    *   **Purpose**: Defines the base limit for concurrent replication commands that the SCM will *send* to a single DataNode.
    *   **Default**: `20`.
    *   **Details**: The effective limit for a decommissioning DataNode is this value multiplied by `hdds.datanode.replication.outofservice.limit.factor`.

##### DataNode-Side Properties

*   **`hdds.datanode.replication.outofservice.limit.factor`**
    *   **Purpose**: A multiplier to increase replication capacity for `DECOMMISSIONING` or `MAINTENANCE` nodes. This is a key property for tuning decommission speed.
    *   **Default**: `2.0`.
    *   **Details**: Although this is a DataNode property, it must also be set in the SCM's configuration. The SCM uses it to send more replication commands, and the DataNode uses it to increase its internal resources (threads and queues) to handle the increased load.

*   **`hdds.datanode.replication.queue.limit`**
    *   **Purpose**: Sets the base size of the queue for incoming replication requests on a DataNode.
    *   **Default**: `4096`.
    *   **Details**: For decommissioning nodes, this limit is scaled by `hdds.datanode.replication.outofservice.limit.factor`.

*   **`hdds.datanode.replication.streams.limit`**
    *   **Purpose**: Sets the base number of threads for the replication thread pool on a DataNode.
    *   **Default**: `10`.
    *   **Details**: For decommissioning nodes, this limit is also scaled by `hdds.datanode.replication.outofservice.limit.factor`.

By tuning these properties, administrators can balance the decommissioning speed against the impact on the cluster's performance.

#### Metrics

The following metrics can be used to monitor the progress of DataNode decommissioning. The names in parentheses are the corresponding Prometheus metric names, which may vary slightly depending on the metrics sink configuration.

##### SCM-side Metrics (`ReplicationManagerMetrics`)

These metrics are available on the SCM and provide a cluster-wide view of the replication process. During decommissioning, you should see an increase in these metrics. The name in parentheses is the corresponding Prometheus metric name.

*   `InflightReplication` (`replication_manager_metrics_inflight_replication`): The number of container replication requests currently in progress.
*   `replicationCmdsSentTotal` (`replication_manager_metrics_replication_cmds_sent_total`): The total number of replication commands sent to DataNodes.
*   `replicasCreatedTotal` (`replication_manager_metrics_replicas_created_total`): The total number of container replicas successfully created.
*   `replicateContainerCmdsDeferredTotal` (`replication_manager_metrics_replicate_container_cmds_deferred_total`): The number of replication commands deferred because source DataNodes were overloaded. If this value is high, it might indicate that the source DataNodes (including the decommissioning one) are too busy.

##### Datanode-side Metrics (`MeasuredReplicator` metrics)

These metrics are available on each DataNode. For a decommissioning node, they show its activity as a source of replicas. For other nodes, they show their activity as targets. The name in parentheses is the corresponding Prometheus metric name.

*   `success` (`measured_replicator_success`): The number of successful replication tasks.
*   `successTime` (`measured_replicator_success_time`): The total time spent on successful replication tasks.
*   `transferredBytes` (`measured_replicator_transferred_bytes`): The total bytes transferred for successful replications.
*   `failure` (`measured_replicator_failure`): The number of failed replication tasks.
*   `failureTime` (`measured_replicator_failure_time`): The total time spent on failed replication tasks.
*   `failureBytes` (`measured_replicator_failure_bytes`): The total bytes that failed to be transferred.
*   `queueTime` (`measured_replicator_queue_time`): The total time tasks spend in the replication queue. A high value might indicate the DataNode is overloaded.

By monitoring these metrics, administrators can get a clear picture of the decommissioning progress and identify potential bottlenecks.

# OM Decommission

Ozone Manager (OM) decommissioning is the process in which you gracefully remove one of the OM from the OM HA Ring.

To decommission an OM and remove the node from the OM HA ring, the following steps need to be executed.
1. Add the _OM NodeId_ of the OM Node to be decommissioned to the _ozone.om.decommissioned.nodes.<omServiceId>_ property in _ozone-site.xml_ of all
   other OMs.
2. Run the following command to decommission an OM node.
```shell
ozone admin om decommission -id=<om-service-id> -nodeid=<decommissioning-om-node-id> -hostname=<decommissioning-om-node-address> [optional --force]
```
The _force_option will skip checking whether OM configurations in _ozone-site.xml_ have been updated with the decommissioned node added to
_**ozone.om.decommissioned.nodes**_ property. <p>**Note -** It is recommended to bootstrap another OM node before decommissioning one to maintain HA.</p>

# SCM Decommission

Storage Container Manager (SCM) decommissioning is the process in which you can gracefully remove one of the SCM from the SCM HA Ring.

To decommission a SCM and remove the node from the SCM HA ring, the following steps need to be executed.
```shell
ozone admin scm decommission [-hV] [--service-id=<scmServiceId>] -nodeid=<nodeId>
```
You can obtain the 'nodeId' by executing this command, **"ozone admin scm roles"**

### Leader SCM
If you want to decommission the **leader** scm, you must first transfer the leadership to a different scm and then decommission the node.

To transfer the leader, we can excute below command,
```shell
ozone admin scm transfer [--service-id=<scmServiceId>] -n=<nodeId>
```
After successful leadership change you can proceed with decommissioning.

### Primordial SCM
If you want to decommission the **primordial** scm, you have to change the _ozone.scm.primordial.node.id_ property to point to a different SCM and then proceed with decommissioning.


### Note
During SCM decommissioning the private key of the decommissioned SCM should be manually deleted. The private keys can be found inside _hdds.metadata.dir_.

Manual deletion is needed until we have certificate revocation support (HDDS-8399)
