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

import { NUResponse } from '@/v2/types/namespaceUsage.types';

export const RootNamespaceUsage: NUResponse = {
  status: 'OK',
  path: '/',
  size: 10000,
  sizeWithReplica: -1,
  subPathCount: 2,
  sizeDirectKey: -1,
  subPaths: [
    { path: '/vol1', size: 6000, sizeWithReplica: -1, isKey: false },
    { path: '/vol2', size: 4000, sizeWithReplica: -1, isKey: false }
  ]
};

export const Vol1NamespaceUsage: NUResponse = {
  status: 'OK',
  path: '/vol1',
  size: 6000,
  sizeWithReplica: -1,
  subPathCount: 1,
  sizeDirectKey: 0,
  subPaths: [
    { path: '/vol1/bucket1', size: 6000, sizeWithReplica: -1, isKey: false }
  ]
};

export const Bucket1NamespaceUsage: NUResponse = {
  status: 'OK',
  path: '/vol1/bucket1',
  size: 6000,
  sizeWithReplica: -1,
  subPathCount: 1,
  sizeDirectKey: 3000,
  subPaths: [
    { path: '/vol1/bucket1/key1', size: 3000, sizeWithReplica: -1, isKey: true }
  ]
};

export const Key1NamespaceUsage: NUResponse = {
  status: 'OK',
  path: '/vol1/bucket1/key1',
  size: 3000,
  sizeWithReplica: 9000,
  subPathCount: 0,
  sizeDirectKey: 3000,
  subPaths: []
};

export const PathNotFoundUsage: NUResponse = {
  status: 'PATH_NOT_FOUND',
  path: '/missing',
  size: 0,
  sizeWithReplica: 0,
  subPathCount: 0,
  sizeDirectKey: 0,
  subPaths: []
};

export const pathNotFoundUsage = (path: string): NUResponse => ({
  ...PathNotFoundUsage,
  path
});

export const InitializingUsage: NUResponse = {
  status: 'INITIALIZING',
  path: '/',
  size: 0,
  sizeWithReplica: 0,
  subPathCount: 0,
  sizeDirectKey: 0,
  subPaths: []
};

export const InitializingSummaryResponse = {
  status: 'INITIALIZING',
  path: '/',
  type: 'ROOT',
  countStats: {
    numVolume: 0,
    numBucket: 0,
    numDir: 0,
    numKey: 0
  },
  objectInfo: {}
};

export const InitializingQuotaResponse = {
  status: 'INITIALIZING'
};

export const RootSummaryResponse = {
  status: 'OK',
  path: '/',
  type: 'ROOT',
  countStats: {
    numVolume: 2,
    numBucket: 0,
    numDir: 0,
    numKey: 0
  },
  objectInfo: {}
};

export const Vol1SummaryResponse = {
  status: 'OK',
  path: '/vol1',
  type: 'VOLUME',
  countStats: {
    numVolume: 0,
    numBucket: 1,
    numDir: 0,
    numKey: 0
  },
  objectInfo: {
    volumeName: 'vol1',
    name: 'vol1'
  }
};

export const Bucket1SummaryResponse = {
  status: 'OK',
  path: '/vol1/bucket1',
  type: 'BUCKET',
  countStats: {
    numVolume: 0,
    numBucket: 0,
    numDir: 0,
    numKey: 1
  },
  objectInfo: {
    bucketName: 'bucket1',
    volumeName: 'vol1',
    name: 'bucket1',
    bucketLayout: 'FILE_SYSTEM_OPTIMIZED',
    owner: 'hdfs',
    storageType: 'DISK',
    usedNamespace: 1
  }
};

export const Key1SummaryResponse = {
  status: 'OK',
  path: '/vol1/bucket1/key1',
  type: 'KEY',
  countStats: {
    numVolume: 0,
    numBucket: 0,
    numDir: 0,
    numKey: 0
  },
  objectInfo: {
    keyName: 'key1',
    fileName: 'key1',
    bucketName: 'bucket1',
    volumeName: 'vol1',
    name: 'key1',
    creationTime: 1673415921920,
    modificationTime: 1673415921920
  }
};

export const RootQuotaResponse = {
  status: 'OK',
  allowed: 10000,
  used: 10000
};

export const Vol1QuotaResponse = {
  status: 'OK',
  allowed: 1073741824,
  used: 6000
};

export const Bucket1QuotaResponse = {
  status: 'OK',
  allowed: 1073741824,
  used: 6000
};

export const Key1QuotaResponse = {
  status: 'TYPE_NOT_APPLICABLE'
};

export const PathNotFoundSummaryResponse = {
  status: 'PATH_NOT_FOUND',
  path: '/missing',
  type: 'UNKNOWN',
  countStats: {
    numVolume: 0,
    numBucket: 0,
    numDir: 0,
    numKey: 0
  },
  objectInfo: {}
};

export const PathNotFoundQuotaResponse = {
  status: 'PATH_NOT_FOUND'
};

export const pathNotFoundSummary = (path: string) => ({
  ...PathNotFoundSummaryResponse,
  path
});
