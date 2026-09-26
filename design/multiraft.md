---
title: Ozone multi-raft support 
summary: Datanodes can be part of multiple independent RAFT groups / pipelines
date: 2019-05-21
jira: HDDS-1564
status: implemented
author: Li Cheng, Sammi Chen
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

 Without multiraft support one datanode could be part of one RAFT group. With multiraft one datanode can be part of multiple independent RAFT ring.
  
# Link

 https://issues.apache.org/jira/secure/attachment/12990694/multiraft_performance_brief.pdf

 https://issues.apache.org/jira/secure/attachment/12969227/Ozone%20Multi-Raft%20Support.pdf