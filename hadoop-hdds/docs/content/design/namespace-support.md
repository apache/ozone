---
title: Ozone FS namespace / prefix table
summary: Use additional prefix table for indexed data retrieval
date: 2021-04-12
jira: HDDS-2939
status: implemented
author: Supratim Deka, Anu Engineer, Rakesh Radhakrishnan
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

 Flat namespace (like key -> key info) is not efficient for listing/deleting/renaming directories. (Large segments should be scanned, the whole sub-hierarchy). To make deletion / rename fast and atomic (and make the lists faster) the key table is separated for prefix + key table.

# Link

 * [Design doc](https://issues.apache.org/jira/secure/attachment/12991926/Ozone%20FS%20Namespace%20Proposal%20v1.0.docx)
 * [Quick overview](https://issues.apache.org/jira/secure/attachment/13023399/OzoneFS%20Optimizations_DesignOverview_%20HDDS-2939.pdf)
