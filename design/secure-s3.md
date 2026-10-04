---
title: Secure S3 keys management
summary: Improving security regarding s3 keys management
date: 2023-03-10
jira: HDDS-8132
status: implementing
author: Maksim Myskov, Mikhail Pochatkin
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

There are problems related to the current S3 keys management:
* Storing keys as plain text in Ozone Manager rocksdb is insecure. An ozone administrator can easily impersonate any user by recovering his keys from rocksdb.
* The only way for a user to generate keys is to have SSH access to the Ozone cluster. Security policies can also prohibit this.
* Keys revocation process is manual which leads to security issues.

We intend to extend Ozone S3 secret key management:
* Support centralized remote S3 secret storage.
* Implement S3 gateway endpoint for getting, renewing and revoking secrets.
* Add TTL to secrets.

This document proposes solutions to the above issues.

# Link

https://issues.apache.org/jira/secure/attachment/13057463/Secure%20S3%20keys%20management.pdf