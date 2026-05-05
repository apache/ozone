---
title: Full Volume Handling
summary: Immediately trigger Datanode heartbeat on detecting full volume
date: 2025-05-12
jira: HDDS-12929
status: implemented 
author: Siddhant Sangwan, Sumit Agrawal
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

> **Note**: The feature described here was implemented in [pull request #8590](https://github.com/apache/ozone/pull/8590). This document reflects the final, merged design.

## Summary
Trigger datanode heartbeat immediately when the container being written to is (close) to full, or volume is full, 
or container is unhealthy, while handling a write request. The immediate heartbeat will contain close container action. 
The overall objective is to avoid Datanode disks from getting completely full and preventing degradation of write 
performance. 

## Problem
When a Datanode volume is close to full, the SCM may not be immediately aware of this because storage reports are only
sent to it every one minute (`HDDS_NODE_REPORT_INTERVAL_DEFAULT = "60s"`). Additionally, SCM only has stale
information about the current size of a container because container size is only updated when an Incremental Container
Report (event based, for example when a container transitions from open to closing state) is received or a Full
Container Report (`HDDS_CONTAINER_REPORT_INTERVAL_DEFAULT = "60m"`) is received. This can lead to the SCM
over-allocating blocks to containers on a Datanode volume that has already reached the min free space boundary. 

In the future, in https://issues.apache.org/jira/browse/HDDS-12151 we plan to fail writes for containers on a volume 
that has reached the min free space boundary. 
So, in the future, when the Datanode fails writes for such a volume, overall write performance will drop because the 
client will have to request for a different set of blocks.

Before this change, a close container action was queued to be sent in the next heartbeat, when:
1. Container was at 90% capacity.
2. Volume was full, __counting committed space__. That is `available - reserved - committed - min free space <= 0`.
3. Container was `UNHEALTHY`.

But since the next heartbeat could be sent up to 30 seconds later in the worst case, this reaction time was too slow. 
This design proposes sending Datanode heartbeat immediately so SCM can get the close container action 
immediately. This will help in reducing performance drop because of write failures when 
https://issues.apache.org/jira/browse/HDDS-12151 is implemented in the future.

### The definition of a full volume
Previously, a volume was considered full if the following method returned true. It accounts for available space, 
committed space, min free space and reserved space (`available` already considers `reserved` space):
```java
  private static long getUsableSpace(
      long available, long committed, long minFreeSpace) {
    return available - committed - minFreeSpace;
  }
```
Counting committed space here, _when sending close container action_, is a bug - we only want to close the container if 
`available - reserved - minFreeSpace <= 0`. 

## Non Goals
Failing the write if it exceeds the min free space boundary (https://issues.apache.org/jira/browse/HDDS-12151) is not 
discussed here.

## Proposed Solution

### Proposal for immediately triggering Datanode heartbeat

We will immediately trigger the heartbeat when:
1. The container is (close) to full (this is existing behaviour, the container full check already exists).
2. The volume is __full EXCLUDING committed space__ (`available - reserved available - min free <= 0`). This is because 
   when a volume 
   is full INCLUDING committed space (`available - reserved available - committed - min free <= 0`), open containers 
   can still accept writes. So the current behaviour of sending a close container action when volume is full including 
   committed space is a bug.
3. The container is unhealthy (this is existing behaviour).

Logic to trigger a heartbeat immediately already exists - we just need to call the method when needed. So, in 
`HddsDispatcher`, when handling a request: 
1. For every write request:
   1. Check the above three conditions
      1. If true, queue the `ContainerAction` to the context as before, then immediately trigger the heartbeat using:
```
context.getParent().triggerHeartbeat();
```

#### Throttling
Throttling is required so the Datanode doesn't send multiple immediate heartbeats for the same container. We can have 
per-container throttling, where we trigger an immediate heartbeat once for a particular container, and after that 
container actions for that container can only be sent in the regular, scheduled heartbeats. Meanwhile, immediate 
heartbeats can still be triggered for different containers.

Here's a visualisation to show this. The letters (A, B, C etc.) denote events and timestamp is the time at which 
an event occurs.
```
Write Call 1:
/ A, timestamp: 0/-------------/B, timestamp: 5/

Write Call 2, in-parallel with 1:
------------------------------ /C, timestamp: 5/

Write Call 3, in-parallel with 1 and 2:
---------------------------------------/D, timestamp: 7/

Write Call 4:
------------------------------------------------------------------------/E, timestamp: 30/

Events:
A: Last, regular heartbeat
B: Volume 1 detected as full while writing to Container 1, heartbeat triggered
C: Volume 1 again detected as full while writing to Container 2, heartbeat triggered
D: Container 1 detected as full, heartbeat throttled
E: Volume 1 detected as full while writing to Container 2, Container Action sent in regular heartbeat 
```

A simple and thread safe way to implement this is to have an `AtomicBoolean` for each container in the `ContainerData` 
class that can be used to check if an immediate heartbeat has already been triggered for that container. The memory 
impact of introducing a new member variable in `ContainerData` for each container is quite small. Consider:

- A Datanode with 600 TB disk space.
- Container size is 5 GB.
- Number of containers = 600 TB / 5 GB = 120,000.
- For each container, we'll have an `AtomicBoolean`, which just has one field that counts: `private volatile int value`.
  - Static fields don't count.
- An int is 4 bytes in Java. Assuming that other overhead for a Java object brings the size of the object to ~20 bytes.
- 20 bytes * 120,000 = ~2.4 MB.

For code implementation, see https://github.com/apache/ozone/pull/8590.

## Alternatives
### Preventing over allocation of blocks in the SCM
The other part of the problem is that SCM has stale information about the size of the container and ends up
over-allocating blocks (beyond the container's 5 GB size) to the same container. Solving this problem is complicated.
We could track how much space we've allocated to a container in the SCM - this is doable on the surface but won't
actually work well. That's because SCM is asked for a block (256 MB), but SCM doesn't know how much data a client will
actually write to that block file. The client may only write 1 MB, for example. So SCM could track that it has already
allocated 5 GB to a container, and will open another container for incoming requests, but the client may actually only
write 1GB. This would lead to a lot of open containers when we have 10k requests/second.

At this point, we've decided not to do anything about this.

### Regularly sending open container reports
Sending open container reports regularly (every 30 seconds for example) can help a little bit, but won't solve the
problem. We won't take this approach for now.

### Sending storage reports in immediate heartbeats
We considered triggering an immediate heartbeat every time the Datanode detects a volume is full 
while handling a write request, with per volume throttling. To each immediate heartbeat, we would the storage 
reports of all volumes in the Datanode + Close Container Action for the particular container being written to. While 
this would update the storage stats of a volume in the SCM faster, which the SCM can subsequently use to decide 
whether to include that Datanode in a new pipeline, the per volume throttling has a drawback. It wouldn't let us 
send close container actions for other containers. We decided not to take this approach.  

## Implementation Plan
1. HDDS-13045: For triggering immediate heartbeat. Already merged - https://github.com/apache/ozone/pull/8590.
2. HDDS-12151: Fail a write call if it exceeds min free space boundary (not the focus of this doc, just mentioned 
   here).
