/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export const ClusterState = {
  "deletedDirs": 0,
  "pipelines": 7,
  "totalDatanodes": 5,
  "healthyDatanodes": 3,
  "storageReport": {
    "capacity": 1352149585920,
    "used": 822805801,
    "remaining": 1068824879104,
    "committed": 12000222315
  },
  "containers": 20,
  "missingContainers": 2,
  "openContainers": 8,
  "deletedContainers": 10,
  "volumes": 2,
  "buckets": 24,
  "keys": 1424,
  "keysPendingDeletion": 2
}

export const OpenKeys = {
  "totalUnreplicatedDataSize": 4096,
  "totalReplicatedDataSize": 1024,
  "totalOpenKeys": 10
}

export const DeletePendingSummary = {
  "totalUnreplicatedDataSize": 4096,
  "totalReplicatedDataSize": 1024,
  "totalDeletedKeys": 3
}
