---
title: SCM HA support
summary: HA for Storage Container Manager using Ratis to replicate data
date: 2020-03-05
jira: HDDS-2823
status: implementing
author: Li Cheng, Nandakumar Vadivelu, Rui Wang, Glen Geng, Shashikant Banerjee
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

# Abstract

 Proposal to implement HA similar to the OM HA: Using Apache Ratis to propagate the 
 
# Links

The main SCM HA design doc is available from [here](https://docs.google.com/document/d/1vr_z6mQgtS1dtI0nANoJlzvF1oLV-AtnNJnxAgg69rM/edit?usp=sharing)

During the implementation of SCM-HA many smaller design docs are created specific to various areas:

 * [SCM HA Distributed Sequence ID Generator](https://docs.google.com/document/d/1LaXz_mjeXPmIKys3oogxQSDLVQOzewpIp3baPGT0Vqw/edit): about generating unique identifier across multiple nodes of the HA quorum
 * [SCM HA Service Manager](https://docs.google.com/document/d/1DbbqP0m3g_iEpY9qkSGOuQgcCN-QqlSNgWpvBOLv5h0/edit): about starting and stopping the main SCM services (like PipelienManager, ReplicationManager) in case of a failover
 * [SCM HA SCMContext](https://docs.google.com/document/d/1h_3gpC4o2EpuBlcQiJC7MMoZz9JmaMX9CxObSxWU614/edit): about using a helper object which includes all the key information for all the required service components
 * [SCM HA Snapshots](https://docs.google.com/document/d/1uy4_ER2V6nNQJ7_5455Wz8NmI142JHPnif6Y1OdPi8E/edit): about RAFT state-machine snapshots
 * [SCM HA: DeleteBlockLog](https://docs.google.com/document/d/166Aea2EowSGWtAFWNlDv0gu4rA06dQ2rJAsBd-l210Q/edit): about coordinating block deletions in HA environment
 * [SCM HA: bootstrap](https://issues.apache.org/jira/secure/attachment/13021254/SCM%20HA%20Bootstrap_updated.pdf): about initializing the SCM HA cluster