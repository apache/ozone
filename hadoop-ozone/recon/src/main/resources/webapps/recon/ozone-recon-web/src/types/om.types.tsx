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

export interface IAcl {
  type: string;
  name: string;
  scope: string;
  aclList: string[];
}

export interface IVolume {
  volume: string;
  owner: string;
  admin: string;
  creationTime: number;
  modificationTime: number;
  quotaInBytes: number;
  quotaInNamespace: number;
  usedNamespace: number;
  acls?: IAcl[];
}

export interface IBucket {
  volumeName: string;
  bucketName: string;
  isVersionEnabled: boolean;
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
  acls?: IAcl[];
  bucketLayout: BucketLayout;
}

// Corresponds to OzoneManagerProtocolProtos.StorageTypeProto
export const BucketStorageTypeList = ['RAM_DISK', 'SSD', 'DISK', 'ARCHIVE'];
type BucketStorageType = typeof BucketStorageTypeList;
export type BucketStorage = BucketStorageType[number];

// Corresponds to OzoneManagerProtocolProtos.BucketLayoutProto
export const BucketLayoutTypeList = ['FILE_SYSTEM_OPTIMIZED', 'OBJECT_STORE', 'LEGACY'];
type BucketLayoutType = typeof BucketLayoutTypeList;
export type BucketLayout = BucketLayoutType[number];

export const ACLIdentityTypeList = ['USER', 'GROUP', 'WORLD', 'ANONYMOUS', 'CLIENT_IP'];
type ACLIdentityType = typeof ACLIdentityTypeList;
export type ACLIdentity = ACLIdentityType[number];

export const ACLRightList = ['READ', 'WRITE', 'CREATE', 'LIST', 'DELETE', 'READ_ACL', 'WRITE_ACL', 'ALL', 'NONE'];
type ACLRightType = typeof ACLRightList;
export type ACLRight = ACLRightType[number];
