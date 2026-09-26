---
title: Configless Ozone service management
summary: Distribute only minimal configuration and download all the remaining before start
date: 2019-05-25
jira: HDDS-1467
status: accepted
author: Márton Elek, Anu Engineer
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

 Configuration keys are partitioned to two parts: runtime settings and environment settings.
 
 Environment settings (hosts, ports) can be downloaded at start.
 
# Link

  * https://issues.apache.org/jira/secure/attachment/12966992/configless.pdf