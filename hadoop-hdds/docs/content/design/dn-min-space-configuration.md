---
title: Minimum free space configuration for datanode volumes
summary: Describe proposal for minimum free space configuration which volume must have to function correctly.
date: 2025-05-05
jira: HDDS-12928
status: implemented
author: Sumit Agrawal
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
Volume in the datanode stores the container data and metadata (rocks db co-located on the volume).
There are various parallel operation going on such as import container, export container, write and delete data blocks,
container repairs, create and delete containers. The space is also required for volume db to perform compaction at regular interval.
This is hard to capture exact usages and free available space. So, this is required to configure minimum free space
so that datanode operation can perform without any corruption and environment being stuck and support read of data.

This free space is used to ensure volume allocation if `required space < (volume available space - free space - reserved space - committed space)`.
Any container creation and import container need to ensure that this constraint is met. And block byte writes need ensure that `free space` space is available.
Note: Any issue related to ensuring free space is tracked with separate JIRA.

# Existing configuration (before HDDS-12928)
Two configurations are provided,
- hdds.datanode.volume.min.free.space  (default: 5GB)
- hdds.datanode.volume.min.free.space.percent

1. If nothing is configured, takes default value as 5GB
2. if both are configured, priority to hdds.datanode.volume.min.free.space
3. else respective configuration is used.

# Problem Statement

- With 5GB default configuration, its not avoiding full disk scenario due to error in ensuring free space availability.
This is due to container size being imported is 5GB which is near boundary, and other parallel operation.
- Volume DB size can increase with increase in disk space as container and blocks it can hold can more and hence metadata.
- Volume DB size can also vary due to small files and big files combination, as more small files can lead to more metadata.

Solution involves
- appropriate default min free space
- depends on disk size variation

# Approach 1 Combination of minimum free space and percent increase on disk size

Configuration:
1. Minimum free space: hdds.datanode.volume.min.free.space: default value `20GB`
2. disk size variation: hdds.datanode.volume.min.free.space.percent: default 0.1% or 0.001 ratio

Minimum free space = Max (`<Min free space>`, `<percent disk space>`)

| Disk space | Min Free Space (percent: 1%) | Min Free Space ( percent: 0.1%) |
| -- |------------------------------|---------------------------------|
| 100 GB | 20 GB                        | 20 GB (min space default)       |
| 1 TB | 20 GB                        | 20 GB (min space default)       |
| 10 TB | 100 GB                       | 20 GB  (min space default) |
| 100 TB | 1 TB                         | 100 GB                          |

considering above table with this solution,
- 0.1 % to be sufficient to hold almost all cases, as not observed any dn volume db to be more that 1-2 GB

# Approach 2 Only minimum free space configuration

Considering above approach, 20 GB as default should be sufficient for most of the disk, as usually disk size is 10-15TB as seen.
Higher disk is rarely used, and instead multiple volumes are attached to same DN with multiple disk.

Considering this scenario, Minimum free space: `hdds.datanode.volume.min.free.space` itself is enough and
percent based configuration can be removed.

### Compatibility
If `hdds.datanode.volume.min.free.space.percent` is configured, this should not have any impact
as default value is increased to 20GB which will consider most of the use case.

# Approach 3 Combination of maximum free space and percent configuration on disk size

Configuration:
1. Maximum free space: hdds.datanode.volume.min.free.space: default value `20GB`
2. disk size variation: hdds.datanode.volume.min.free.space.percent: default 10% or 0.1 ratio

Minimum free space = **Min** (`<Max free space>`, `<percent disk space>`)
> Difference with approach `one` is, Min function over the 2 above configuration

| Disk space | Min Free Space (20GB, 10% of disk) |
| -- |------------------------------------|
| 10 GB | 1 GB (=Min(20GB, 1GB)              |
| 100 GB | 10 GB (=Min(20GB, 10GB)            |
| 1 TB | 20 GB   (=Min(20GB, 100GB)         |
| 10 TB | 20 GB (=Min(20GB, 1TB)             |
| 100 TB | 20GB  (=Min(20GB, 10TB)            |

This case is more useful for test environment where disk space is less and no need any additional configuration.

# Conclusion
1. Going with Approach 1
- Approach 2 is simple setting only min-free-space, but it does not expand with higher disk size.
- Approach 3 is more applicable for test environment where disk space is less, else same as Approach 2.
- So Approach 1 is selected considering advantage where higher free space can be configured by default.
2. Min Space will be 20GB as default

---

# Soft and Hard Min-Free-Space Limits

## Overview

The min-free-space value chosen above is split into two distinct thresholds that serve different purposes:
a **soft** (reported) limit and a **hard** (locally-enforced) limit. The gap between them is the
*soft band* — a warning zone where writes are still accepted but the Datanode is already signalling
to SCM that it is running low.

## Configuration

| Key | Default | Purpose |
|-----|---------|---------|
| `hdds.datanode.volume.min.free.space` | `20GB` | Absolute floor shared by both tiers. Effective spare = `max(this, capacity × ratio)`. |
| `hdds.datanode.volume.min.free.space.percent` | `2%` | **Soft limit ratio** — reported to SCM via `freeSpaceToSpare` in storage heartbeats. |
| `hdds.datanode.volume.min.free.space.hard.limit.percent` | `1.5%` | **Hard limit ratio** — enforced locally for write rejection and new container placement. |

Rules:
- `hard.limit.percent` must be ≤ `min.free.space.percent`. If it is set higher, it is silently
  clamped down to the soft ratio, making the soft band width zero (effectively disabling it).
- Setting both ratios to the same value disables the soft band entirely — behaviour is then identical
  to the pre-split single-threshold code.

### Effective spare values for a 2 TB volume

| Threshold | Calculation | Result |
|-----------|-------------|--------|
| Soft (reported) | `max(20 GB, 2 TB × 2%)` | **40 GB** |
| Hard (local)    | `max(20 GB, 2 TB × 1.5%)` | **30 GB** |
| Soft band width | `40 GB − 30 GB` | **10 GB** |

## How Each Limit Is Used

### Hard limit — local write enforcement and new container placement

`getFreeSpaceToSpare(capacity)` returns the hard spare. It is checked in two places:

1. **Write rejection** (`ContainerUtils.assertSpaceAvailability`): a write is rejected with
   `DISK_OUT_OF_SPACE` when `available − hardSpare < requestedBytes`. This is the definitive gate
   that prevents disk exhaustion.

2. **New container placement** (`AvailableSpaceFilter`, `PendingContainerTracker`): SCM only
   schedules a new container on a volume when `available − committed − hardSpare > maxContainerSize`.
   This ensures that containers are placed only where writes will actually succeed.

### Soft limit — SCM visibility and proactive close-container actions

`getReportedFreeSpaceToSpare(capacity)` returns the soft spare. It appears in two places:

1. **Storage heartbeat** (`HddsVolume.reportBuilder`): each storage report sent from the Datanode to
   SCM carries `freeSpaceToSpare = softSpare`. SCM uses this value to gauge how much usable space
   remains on the volume when making placement decisions for new pipelines and replication.

2. **Soft-band metric** (`ContainerUtils.assertSpaceAvailability`, `AvailableSpaceFilter`):
   when a write (or container placement attempt) succeeds the hard check but `available − softSpare`
   is below the required size, the Datanode increments
   `numWriteRequestsInSoftBandMinFreeSpace` (for writes) or
   `numContainerCreateRequestsInSoftBandMinFreeSpace` (for container placement). These metrics
   alert operators that the volume is inside the warning zone.

## The Soft Band: Improving Write Continuity Near Capacity Limits

Without a soft band, a single threshold would have to serve two conflicting goals:
- **High enough** to give SCM enough lead time to stop routing work to a nearly-full node.
- **Low enough** to maximise usable disk space and avoid premature write rejection.

Splitting the threshold resolves the conflict:

```
  Disk capacity
  ─────────────────────────────────────────────────────────────────
  usable data space (writes accepted here)
  ─────────────────────────────────────────── ← hard spare (30 GB)
  soft band (10 GB warning zone):
    writes still accepted, but DN metrics fire and SCM starts
    steering new work away from this volume
  ─────────────────────────────────────────── ← soft spare (40 GB)
  reserved buffer (SCM sees this as "full")
  ─────────────────────────────────────────────────────────────────
```

**What happens as a volume fills up:**

1. **Above soft spare (> 40 GB free):** Normal operation.

2. **Inside the soft band (30 – 40 GB free):** Writes to existing open containers still succeed.
   The `InSoftBand` metrics increment, and because SCM already sees `freeSpaceToSpare = 40 GB` via
   the heartbeat, it stops preferring this volume for new pipelines. The Datanode may send
   close-container actions (see [Full Volume Handling](full-volume-handling.md)) to speed up
   migration.

3. **Below hard spare (< 30 GB free):** Writes are rejected. The `RejectedHardMinFreeSpace` metrics
   increment. No new containers are placed on this volume by SCM.

**Why this improves write continuity:** without the band, SCM would only learn a volume is nearly
full when write rejections start happening. With the band, SCM gets advance warning via a smaller
reported `freeSpaceToSpare` and can route new containers to other volumes *before* the hard limit is
hit, reducing client-visible write failures.

## Turning Off the Soft Band

To disable the soft band (equivalent to single-threshold behaviour), set the two ratios to the same
value:

```xml
<property>
  <name>hdds.datanode.volume.min.free.space.percent</name>
  <value>0.02</value>
</property>
<property>
  <name>hdds.datanode.volume.min.free.space.hard.limit.percent</name>
  <value>0.02</value>
</property>
```

When both ratios are equal, `softSpare == hardSpare`, the soft band width is zero, and the
`InSoftBand` metrics never fire.


