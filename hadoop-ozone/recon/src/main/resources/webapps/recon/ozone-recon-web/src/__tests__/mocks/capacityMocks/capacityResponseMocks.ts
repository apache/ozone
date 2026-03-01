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

export const StorageDistribution = {
  globalStorage: {
    totalFileSystemCapacity: 11264,
    totalOzoneUsedSpace: 4096,
    totalOzoneFreeSpace: 4096,
    totalOzoneCapacity: 10240,
    totalReservedSpace: 1024,
    totalOzonePreAllocatedContainerSpace: 1024
  },
  globalNamespace: {
    totalUsedSpace: 4096,
    totalKeys: 12
  },
  usedSpaceBreakdown: {
    openKeyBytes: {
      totalOpenKeyBytes: 1024,
      openKeyAndFileBytes: 512,
      multipartOpenKeyBytes: 512
    },
    committedKeyBytes: 2048
  },
  dataNodeUsage: [
    {
      datanodeUuid: 'uuid-1',
      hostName: 'dn-1',
      capacity: 8192,
      used: 4096,
      remaining: 2048,
      committed: 1024,
      minimumFreeSpace: 512,
      reserved: 128
    },
    {
      datanodeUuid: 'uuid-2',
      hostName: 'dn-2',
      capacity: 8192,
      used: 2048,
      remaining: 2048,
      committed: 1024,
      minimumFreeSpace: 256,
      reserved: 128
    }
  ]
};

export const ScmPendingDeletion = {
  totalBlocksize: 1024,
  totalReplicatedBlockSize: 2048,
  totalBlocksCount: 2
};

export const OmPendingDeletion = {
  totalSize: 2048,
  pendingDirectorySize: 1024,
  pendingKeySize: 1024
};

export const DnPendingDeletion = {
  status: "FINISHED",
  totalPendingDeletionSize: 3072,
  pendingDeletionPerDataNode: [
    {
      hostName: 'dn-1',
      datanodeUuid: 'uuid-1',
      pendingBlockSize: 1024
    },
    {
      hostName: 'dn-2',
      datanodeUuid: 'uuid-2',
      pendingBlockSize: 2048
    }
  ],
  totalNodesQueried: 2,
  totalNodeQueriesFailed: 0
};
