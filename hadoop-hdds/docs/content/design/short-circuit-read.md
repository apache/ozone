---
title: Short Circuit Local Read in DN 
summary: Support read data from local disk file directly when the client and data are co-located on the same server
date: 2024-12-04
jira: HDDS-10685
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

Short-circuit local read feature bypasses the Datanode, allowing the client to read the file from the local disk directly when the client is co-located with the data on the same server.
  
# Link

https://issues.apache.org/jira/secure/attachment/13068166/ShortCircuitReadInOzone-V1.pdf