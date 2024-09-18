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

import { Acl } from "@/v2/types/acl.types";
import { Option as MultiOption } from "@/v2/components/select/multiSelect";

// Corresponds to OzoneManagerProtocolProtos.StorageTypeProto
export const BucketStorageTypeList = [
  'RAM_DISK',
  'SSD',
  'DISK',
  'ARCHIVE'
] as const;
export type BucketStorage = typeof BucketStorageTypeList[number];

// Corresponds to OzoneManagerProtocolProtos.BucketLayoutProto
export const BucketLayoutTypeList = [
  'FILE_SYSTEM_OPTIMIZED',
  'OBJECT_STORE',
  'LEGACY'
] as const;
export type BucketLayout = typeof BucketLayoutTypeList[number];


export type Bucket = {
  volumeName: string;
  name: string;
  versioning: boolean;
  storageType: BucketStorage;
  creationTime: number;
  modificationTime: number;
  sourceVolume?: string;
  sourceBucket?: string;
  usedBytes: number;
  usedNamespace: number;
  quotaInBytes: number;
  quotaInNamespace: number;
  owner: string;
  acls?: Acl[];
  bucketLayout: BucketLayout;
}

export type BucketResponse = {
  totalCount: number;
  buckets: Bucket[];
}

export type BucketsState = {
  totalCount: number;
  lastUpdated: number;
  columnOptions: MultiOption[];
  volumeBucketMap: Map<string, Set<Bucket>>;
  bucketsUnderVolume: Bucket[];
  volumeOptions: MultiOption[];
}

export type BucketsTableProps = {
  loading: boolean;
  data: Bucket[];
  handleAclClick: (arg0: Bucket) => void;
  selectedColumns: MultiOption[];
  searchColumn: 'name' | 'volumeName';
  searchTerm: string;
}
