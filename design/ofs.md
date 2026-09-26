---
title: Implement new Ozone FileSystem scheme ofs:// 
summary: A new schema structure for Hadoop compatible file system
date: 2020-06-30
jira: HDDS-2665
status: implemented
author: Siyao Meng 
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

  Scheme: ofs://<Hostname[:Port] or Service ID>/[<volumeName>/<bucketName>/path/to/key]

# Link

Design doc is uploaded to the JIRA HDDS-2665:

* https://issues.apache.org/jira/secure/attachment/12987636/Design%20ofs%20v1.pdf
