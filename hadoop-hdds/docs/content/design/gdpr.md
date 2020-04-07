---
title: Ozone GDPR framework 
summary: Crypto key management to handle GDPR "right to be forgotten" feature
date: 2019-06-20
jira: HDDS-2012
status: implemented
author: Dinesh Chitlangia 
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

 Encrypt all the key and delete only the encryption key if the deletion is requested. Without the key any remaining data can't be read any more.
  
# Link

  https://issues.apache.org/jira/secure/attachment/12978992/Ozone%20GDPR%20Framework.pdf

  https://issues.apache.org/jira/secure/attachment/12987528/Ozone%20GDPR%20Framework_updated.pdf