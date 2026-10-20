---
title: Ozone Manager Prepare for Upgrade 
summary: ‘Prepare upgrade’ step to make sure all the OMs use the same version of the software to update their DBs (apply transaction) for a given request.
date: 2021-02-15
jira: HDDS-4470
status: implemented
author: Aravindan Vijayan 
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

 In the context of upgrades, a slow follower presents a problem when there
  are changes in the OM Ratis request handler logic. This means that when there 
  is a slow follower, it is possible to have different versions of the code 
  processing the request at different OM nodes causing problems from DB divergence 
  to unexpected crashes. For example, a leader could have “applied” a transaction 
  in the older OM version, while a slow follower who has the request in its logs 
  may apply it after the upgrade. 

 The objective of the ‘Prepare upgrade’ step is to make sure all the OMs use
  the same version of the software to update their DBs (apply transaction) for a given request. 
 
 To ensure this, we need to make sure that 
 * For every operational OM (at least a quorum of OMs should be operational
 ), all unapplied transactions should be applied. 
 * For an OM that is not operational during the prepare step, it should get a
  Ratis snapshot (entire OM RocksDB) to get up to speed with the rest of the OMs after the upgrade.

# Usage

## How do you prepare an Ozone manager quorum

    ozone admin om prepare -id=<om-service-id>

This leaves the Ozone manager in a state where it cannot accept new writes.

## How do you cancel a "prepared" Ozone manager quorum

In the case of a cancelled upgrade, the OM can be brought out off the
prepared state by using the following command.

    ozone admin om cancelprepare -id=<om-service-id>

# Link

  https://issues.apache.org/jira/secure/attachment/13015491/OM%20Prepare%20Upgrade.pdf

  https://issues.apache.org/jira/secure/attachment/13015411/OM%20Prepare%20Upgrade%2CDowngrade.jpg
