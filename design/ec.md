---
title: Erasure Coding in Ozone 
summary: Use Erasure Coding algorithm for efficient storage
date: 2020-06-30
jira: HDDS-3816
status: draft
author: Uma Maheswara Rao Gangumalla, Marton Elek, Stephen O'Donnell 
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

 Support Erasure Coding for read and write pipeline of Ozone.
  
# Status

 The design doc describes two main methods to implement EC:
 
  * Container level, async Erasure Coding, to encode closed containers in the background
  * Block level, striped Erasure Coding
 
 Second option can work only with new, dedicated write-path. Details of possible implementation will be included in the next version.
 
# Link

 https://issues.apache.org/jira/secure/attachment/13006245/Erasure%20Coding%20in%20Apache%20Hadoop%20Ozone.pdf

