---
title: Merge Container RocksDB in DN 
summary: Use one RocksDB to hold all container metadata on a DN data volume
date: 2022-05-24
jira: HDDS-3630
status: implemented

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

 Use one single RocksDB to hold all container metadata on a DN data volume, replacing the current one RocksDB for each container mode. 
  
# Link

https://issues.apache.org/jira/secure/attachment/13044057/Merge%20rocksdb%20in%20datanode%20V5.pdf