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

import { UtilizationResponse, SCMPendingDeletion, OMPendingDeletion, DNPendingDeletion } from "@/v2/types/capacity.types";

export const DEFAULT_CAPACITY_UTILIZATION: UtilizationResponse = {
  globalStorage: {
    totalUsedSpace: 0,
    totalFreeSpace: 0,
    totalCapacity: 0
  },
  globalNamespace: {
    totalUsedSpace: 0,
    totalKeys: 0
  },
  usedSpaceBreakdown: {
    openKeyBytes: 0,
    committedKeyBytes: 0,
    preAllocatedContainerBytes: 0
  },
  dataNodeUsage: []
};

export const DEFAULT_SCM_PENDING_DELETION: SCMPendingDeletion = {
  totalBlocksize: 0,
  totalReplicatedBlockSize: 0,
  totalBlocksCount: 0
};

export const DEFAULT_OM_PENDING_DELETION: OMPendingDeletion = {
  totalSize: 0,
  pendingDirectorySize: 0,
  pendingKeySize: 0
};

export const DEFAULT_DN_PENDING_DELETION: DNPendingDeletion = {
  status: "NOT_STARTED",
  totalPendingDeletionSize: 0,
  pendingDeletionPerDataNode: [{
    hostName: 'unknown-host',
    datanodeUuid: 'unknown-uuid',
    pendingBlockSize: 0
  }],
  totalNodesQueried: 0,
  totalNodeQueriesFailed: 0
};
