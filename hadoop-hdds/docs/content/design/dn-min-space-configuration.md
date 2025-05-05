---
title: Minimum free space configuration for datanode volumes
summary: Describe proposal for minimum free space configuration which volume must have to function correctly.
date: 2025-05-5
jira: HDDS-12928
status: draft
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
Volume in datanode stores the container data and metadata of this is present in volume db.
There are various parallel operation going on such as import container, export container, write and delete data blocks,
create and delete containers. The space is also required for volume db to perform compaction at regular interval.
This is hard to capture exact usages and free available space. So, this is required to configure minimum free space
so that datanode operation can perform without any corruption and environment being stuck and support read of data.

This free space is used to ensure volume allocation if `required space < (volume available space - free space - reserved space)`
Any container creation and import container need ensure this constrain is met. And block write need ensure that this space is available if new blocks are written.
Note: Any issue related to ensuring free space is tracked with separate JIRA.

# Existing configuration
Two configuration is provided,
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
1. Minimum free space: hdds.datanode.volume.min.free.space: default value `10GB`
2. disk size variation: hdds.datanode.volume.min.free.space.percent: default 0.1% or 0.001 ratio

Minimum free space = Max (`<Min free space>`, `<percent disk space>`)

| Disk space | Min Free Space (percent: 1%) | Min Free Space ( percent: 0.1%) |
| -- |------------------------------|---------------------------------|
| 100 GB | 10 GB | 10 GB (min space default)       |
| 1 TB | 10 GB | 10 GB (min space default) |
| 10 TB | 100 GB | 10 GB |
| 100 TB |  1 TB | 100 GB |

considering above table with this solution,
- 0.1 % to be sufficient to hold almost all cases, as not observed any dn volume db to be more that 1-2 GB

# Approach 2 Only minimum free space configuration

Considering above approach, 10 GB as default should be sufficient for most of the disk, as usually disk size is 10-15TB as seen.
Higher disk is rarely used, and instead multiple volumes are attached to same DN with multiple disk.

Considering this scenario, Minimum free space: `hdds.datanode.volume.min.free.space` itself is enough and
percent based configuration can be removed.

### Compatibility
If `hdds.datanode.volume.min.free.space.percent` is configured, this should not have any impact
as default value is increased to 10GB which will consider most of use case.

# Approach 3 Combination of maximum free space and percent configuration on disk size

Configuration:
1. Maximum free space: hdds.datanode.volume.min.free.space: default value `10GB`
2. disk size variation: hdds.datanode.volume.min.free.space.percent: default 10% or 0.1 ratio

Minimum free space = **Min** (`<Max free space>`, `<percent disk space>`)
> Difference with approach `one` is, Min function over the 2 above configuration

| Disk space | Min Free Space (10GB, 10% of disk) |
| -- |------------------------------------|
| 10 GB | 1 GB (=Min(10GB, 1GB)              |
| 100 GB | 10 GB (=Min(10GB, 10GB)            |
| 1 TB | 10 GB   (=Min(10GB, 100GB)         |
| 10 TB | 10 GB (=Min(10GB, 1TB)             |
| 100 TB | 10GB  (=Min(10GB, 10TB)            |

This case is more useful for test environment where disk space is less and no need any additional configuration.

# To discuss
1. From above point, we need increase the minimum disk space configuration to 10 GB as default (earlier 5GB)
2. Weather we need `hdds.datanode.volume.min.free.space.percent` configuration or not. If required, weather approach 1 or approach 3.



