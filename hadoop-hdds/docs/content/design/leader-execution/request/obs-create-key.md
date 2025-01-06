---
title: Create key for OBS bucket request flow
summary: Create key for OBS bucket request flow steps for leader side execution
date: 2025-01-06
jira: HDDS-11898
status: draft
author: Sumit Agrawal 
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
# OBS Create key flow

Utility classes:
- DbChangeRecorder: record db changes
- ExecutionContext: provides index and other resources

`class OMKeyCreateObsExecutor`

## preprocess

- validate key format and reserve keyword
- normalize key
- capture original bucket and resolve bucket (if different)


## authorize

Acl validation for volume, resolved and original bucket, and key permission (via ranger or native acl).

## lock
- Read lock for bucket
- key lock is not required as parallel key creation is allowed

## unlock
unlock bucket

## process

- validate if bucket is changed after bucket lock
- validate encryption info if bucket have but key do not have
- retrieve encryption info (MPU / normal case)
- prepare key info
  - get replication config
  - generate object Id from index
  - add block info (if not MPU)
- quota validation at the moment
- add open key to ChangeRecorder
- update metrics and audit log
- prepare response and return

# Old Flow comparison changes
Compare to old flow, below cases are removed,
1. retrieve old key if exist - not required, as during commit, overwrite happens
2. key-rewrite validation: this is not required at this point, as commit already have validation

# Testability

For existing test code, behavior cases can be rewritten with new Test classes with validation.
