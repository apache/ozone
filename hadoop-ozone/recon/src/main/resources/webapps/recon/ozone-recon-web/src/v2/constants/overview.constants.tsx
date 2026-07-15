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

import { ClusterStateResponse, KeysSummary, TaskStatus } from "@/v2/types/overview.types";

export const DEFAULT_CLUSTER_STATE: ClusterStateResponse = {
  missingContainers: 0,
  totalDatanodes: 0,
  healthyDatanodes: 0,
  pipelines: 0,
  storageReport: { capacity: 0, used: 0, remaining: 0, committed: 0 },
  containers: 0,
  volumes: 0,
  buckets: 0,
  keys: 0,
  openContainers: 0,
  deletedContainers: 0,
  keysPendingDeletion: 0,
  scmServiceId: 'N/A',
  omServiceId: 'N/A'
};

export const DEFAULT_TASK_STATUS: TaskStatus[] = [];

export const DEFAULT_OPEN_KEYS_SUMMARY: KeysSummary & {totalOpenKeys: number} = {
  totalUnreplicatedDataSize: 0,
  totalReplicatedDataSize: 0,
  totalOpenKeys: 0
};

export const DEFAULT_DELETE_PENDING_KEYS_SUMMARY: KeysSummary & {totalDeletedKeys: number} = {
  totalUnreplicatedDataSize: 0,
  totalReplicatedDataSize: 0,
  totalDeletedKeys: 0
};
