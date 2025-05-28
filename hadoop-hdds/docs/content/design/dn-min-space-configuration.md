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


