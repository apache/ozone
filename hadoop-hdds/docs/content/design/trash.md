---
title: Ozone Trash Feature
summary: Feature provides a user with the ability to recover keys that may have been deleted accidentally. (similar to the HDFS trash feature).
date: 2019-11-07
jira: HDDS-2416
status: implementing
author: Matthew Sharp
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

The design doc is uploaded to the JIRA: 

https://issues.apache.org/jira/secure/attachment/12985273/Ozone_Trash_Feature.docx

## Special note

Trash is disabled for both o3fs and ofs even if `fs.trash.interval` is set
on purpose. (HDDS-3982)
