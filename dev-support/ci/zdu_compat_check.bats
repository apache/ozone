#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Checks that zdu_compat_check.sh reports the expected warnings for commits from Ozone's master branch.
# Each test names a commit that contains a change of the kind being checked. The whole commit is checked, so it
# may also trigger other warnings, which the tests ignore. This needs a full clone (the bats check in CI uses
# fetch-depth 0).

# Runs the check on the diff of commit $1 and leaves its output in $output.
check_commit() {
  local commit="$1"
  git cat-file -e "${commit}^{commit}" || skip "commit ${commit} is not in this clone. Fetch the full history to run this test."
  # Get the change the commit made against its first parent, and feed that diff to the script under test.
  run "${BATS_TEST_DIRNAME}/zdu_compat_check.sh" < <(git diff "${commit}~1" "${commit}")
  [ "$status" -eq 0 ]
}

@test "no output for empty diff" {
  run "${BATS_TEST_DIRNAME}/zdu_compat_check.sh" < /dev/null
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "no output for commit without incompatible changes" {
  # HDDS-17411. Show one initialization notice on the Namespace Usage page
  check_commit 8f4347592d
  [ -z "$output" ]
}

@test "no output for test files" {
  # HDDS-16747. Fix intermittent timeout in testBucketLockWithPurgeDirectory. Only changes an OM request test.
  check_commit 5123d2f9bc
  [ -z "$output" ]
}

@test "OM request change" {
  # HDDS-17239. Initialize usage counters on bucket create
  check_commit 860d14335e
  [[ "$output" == *"OMBucketCreateRequest.java\`: OM request changed"* ]]
  [[ "$output" == *"@DisallowedUntilOmVersion"* ]]
  [[ "$output" != *"RocksDB write"* ]]
  [[ "$output" != *"Exception added or removed"* ]]
}

@test "OM request exception" {
  # HDDS-17162. Return INVALID_PART for a non-positive multipart part number in OM create key requests
  check_commit 6bb94ed5ca
  [[ "$output" == *"OMKeyRequest.java\`: Exception added or removed"* ]]
}

@test "OM request RocksDB write" {
  # HDDS-16110. [STS] Update key in sts revocation table
  check_commit dc169f6e87
  [[ "$output" == *"S3RevokeSTSTokenRequest.java\`: RocksDB write changed"* ]]
}

@test "client side translator change" {
  # HDDS-16622. Support If-None-Match entity ETags for S3 writes
  check_commit dd2aceb834
  [[ "$output" == *"OzoneManagerProtocolClientSideTranslatorPB.java\`: Client side translator changed"* ]]
  [[ "$output" == *"needs a gate of its own"* ]]
  [[ "$output" != *"Server side translator"* ]]
}

@test "server side translator change" {
  # HDDS-16044. SCM should forward Datanode's current version to clients for read and write operations
  check_commit 4443f64231
  [[ "$output" == *"ScmBlockLocationProtocolServerSideTranslatorPB.java\`: Server side translator changed"* ]]
  [[ "$output" == *"remain backwards compatible with older clients"* ]]
  [[ "$output" == *"impact downgrade must be version gated"* ]]
  [[ "$output" != *"Client side translator"* ]]
}

@test "proto file change without a protolock gap" {
  # HDDS-16197. Remove all bucket tagging outstanding TODOs. Only removes proto lines.
  check_commit 3af0d141eb
  [[ "$output" == *"OmClientProtocol.proto\`: Proto file changed"* ]]
  [[ "$output" != *"Field added"* ]]
  [[ "$output" != *"RPC added"* ]]
  [[ "$output" != *"Enum value"* ]]
}

@test "proto field added" {
  # HDDS-16126. Fence quota repair deltas by bucket objectID
  check_commit 4ab81d2a2e
  [[ "$output" == *"OmClientProtocol.proto\`: Proto file changed"* ]]
  [[ "$output" == *"OmClientProtocol.proto\`: Field added"* ]]
  [[ "$output" != *"Enum value"* ]]
}

@test "proto enum value added" {
  # HDDS-16389. Add command to display snapshot counts per bucket
  check_commit b5af13b8dc
  [[ "$output" == *"OmClientProtocol.proto\`: Enum value added, removed or changed"* ]]
}

@test "proto enum value changed" {
  # HDDS-14876. Make snapshot diff progress more granular. Marks an existing enum value as deprecated.
  check_commit b4a4227cf4
  [[ "$output" == *"OmClientProtocol.proto\`: Enum value added, removed or changed"* ]]
}

@test "proto rpc added" {
  # HDDS-15875. OM should validate software versions of peers before accepting finalize command
  check_commit 53f472a7c0
  [[ "$output" == *"OMAdminProtocol.proto\`: RPC added"* ]]
}

@test "peer protocol change" {
  # HDDS-17075. Throttle OM block deletion to SCM when SCM followers are highly lagged
  check_commit 182fb5f470
  [[ "$output" == *"ScmBlockLocationProtocol.java\`: Internal OM peer, SCM peer, or OM to SCM protocol changed"* ]]
}

@test "DB definition change" {
  # HDDS-16110. [STS] Update key in sts revocation table
  check_commit dc169f6e87
  [[ "$output" == *"OMDBDefinition.java\`: RocksDB database layout changed"* ]]
}

@test "configuration default change in ozone-default.xml" {
  # HDDS-16418. Make Inter-SCM gRPC timeouts and keepalive configurable
  check_commit 24cf9aaa28
  [[ "$output" == *"ozone-default.xml\`: Configuration default changed"* ]]
}

@test "configuration default change in a Java config class" {
  # HDDS-17357. Initialize Netty in background when Ozone client is created
  check_commit 88119243f9
  [[ "$output" == *"OzoneClientConfig.java\`: Configuration default changed"* ]]
}

@test "each warning is reported once per file" {
  # HDDS-17239. Initialize usage counters on bucket create. Adds several lines to one OM request.
  check_commit 860d14335e
  [ "$(grep -c 'OMBucketCreateRequest.java`: OM request changed' <<< "$output")" -eq 1 ]
}

@test "link to the developer guide is included with the warnings" {
  check_commit 860d14335e
  [[ "$output" == *"https://ozone.apache.org/docs/next/developer-guide/develop/upgrade-compatibility"* ]]
}
