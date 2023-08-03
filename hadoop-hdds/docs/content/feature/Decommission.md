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

#DataNode Decommission

The DataNode decommission is the process that removes the existing DataNode from the Ozone cluster while ensuring that the new data should not be written to the decommissioned DataNode. When you initiate the process of decommissioning a DataNode, Ozone automatically ensures that all the storage containers on that DataNode have an additional copy created on another DataNode before the decommission completes.

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

## OM Decommission

Ozone Manager (OM) decommissioning is the process in which you gracefully remove one of the OM from the OM HA Ring.

To decommission an OM and remove the node from the OM HA ring, the following steps need to be executed.
1. Stop the OzoneManager process only on the node which needs to be decommissioned. <p> **Note -** Do not stop the decommissioning OM if there are
   only two OMs in the ring as both the OMs would be needed to reach consensus to update the Ratis configuration.</p>
2. Add the _OM NodeId_ of the to be decommissioned OM node to the _ozone.om.decommissioned.nodes.<omServiceId>_ property in _ozone-site.xml_ of all
   other OMs.
3. Run the following command to decommission an OM node.
```shell
ozone admin om decommission -id=<om-service-id> -nodeid=<decommissioning-om-node-id> -hostname=<decommissioning-om-node-address> [optional --force]
```
The _force_option will skip checking whether OM configurations in _ozone-site.xml_ have been updated with the decommissioned node added to
_**ozone.om.decommissioned.nodes**_ property. <p>**Note -** It is recommended to bootstrap another OM node before decommissioning one to maintain HA.</p>

# SCM Decommission

Storage Container Manager (SCM) decommissioning is the process in which you can gracefully remove one of the SCM from the SCM HA Ring.

To decommission a SCM and remove the node from the SCM HA ring, the following steps need to be executed.
```shell
ozone admin scm decommission [-hV] [-id=<scmServiceId>] -nodeid=<nodeId>
                                    [--scm=<scm>]
```
You can obtain the 'nodeid' by executing this command, **"ozone admin scm roles"**

**Note -** If you want to decommission a **primordial** scm, first change the primordial scm.
You can do it by stopping the scm first using **"ozone --daemon stop scm"** then updating the property **"ozone.scm.primordial.node.id"** with some other scm host in ozone-site.xml and then start the scm using **"ozone --daemon start scm"**
