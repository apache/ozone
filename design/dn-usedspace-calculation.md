---
title: Datanode used space calculation 
summary: Describe proposal for optimization in datanode used space calculation.
date: 2025-04-30
jira: HDDS-12924
status: Implemented
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
Datanode makes use of DU to identify disk space uses for ozone in the volume.
DU is a time-consuming operation if run over large occupied disk, like when running over disk path having 10s of TB container data. This will be slow operation.


## Approach 1: Run DU over non-ozone path

Challenges:

- Root path is not known for the device mounted on. So this needs extra configuration.
- Permission problem: du will not count the space if permission is not there on a certain path.


Based on the above concern, it's `not feasible` to do du over non-ozone path.


## Approach 2: Run DU over meta path only (excluding container dir path)

Ozone space usages includes summation of:
- Sum of all Container data size as present in memory
- DU over volume path as current (excluding container path)

`Used space = sum(<all containers data size>, <du over ozone path excluding container data>)`

### Limitation:
- Space is not counted as ozone space for below cases:
  - Ozone used size for duplicate containers are not counted (ignored during startup for EC case)
  - Ozone used size for containers corrupted are not counted, not deleted
  - Container path meta files like container yaml, exported rocks db’s sst files, are not counted, which might result in few GB of data in a large cluster. 

  These spaces will be added up to non-ozone used space, and especially un-accounted containers need to be cleaned up.

In future, the container space (due to duplicacy or corruption) needs to be removed based on error logs.


### Impact of inaccuracy of Used Space

1. Used space in reporting (may be few GB as per above)
2. Adjustment between Ozone available space and Reserved available space

This inaccuracy does not have much impact over solution (as its present in existing) and due to nature of “du” running async and parallel write operation being in progress.

Approach to fits better in this scenario, can be provided as `OptimizedDU` and keeping previous support as well.

## Approach 3 : Used space will be only container data

Hadoop also have similar concept, where used space is only the actual data blocks, and its calculation is simple,
i.e. 

`Used Space = number of blocks * block Size`

Similar to this, Ozone can also calculate Used space as,

`Used Space = Sum of all container data size`

### Impact:

Space will `not` be counted:
1. Meta space like rocks db, container yaml file and sst files of few KBs per container, which overall result in few GB of space in large volume.
2. duplicate container not yet deleted and corrupted container present in container data path
3. temp path having left over data which is not yet deleted

These space will be counted in reserved space. And these data are small in nature (except container data).
Container data are corrupted / <to be deleted> containers which needs to be removed manually or need fix in code to remove those automatic.

So considering above impact, this might be one of simple solution providing higher performance.

# Conclusion

`Approach 2` `Run DU over meta path only (excluding container dir path)` is preferred over other as
it identify ozone used space more close in accuracy to DU implementation and handle the time issue.
