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

**Note:** To recommission a datanode you may execute the below command in cli,
```shell
ozone admin datanode recommission [-hV] [-id=<scmServiceId>]
       [--scm=<scm>] [<hosts>...]
```

# OM Decommission

Ozone Manager (OM) decommissioning is the process in which you gracefully remove one of the OM from the OM HA Ring.

To decommission an OM and remove the node from the OM HA ring, the following steps need to be executed.
1. Add the _OM NodeId_ of the to be decommissioned OM node to the _ozone.om.decommissioned.nodes.<omServiceId>_ property in _ozone-site.xml_ of all
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
