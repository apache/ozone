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

import { Option } from "@/v2/components/select/multiSelect";

export type FileCountResponse = {
  volume: string;
  bucket: string;
  fileSize: number;
  count: number;
}

export type ContainerCountResponse = {
  containerSize: number;
  count: number;
}

export type PlotResponse = {
  fileCountResponse: FileCountResponse[],
  containerCountResponse: ContainerCountResponse[]
}

export type FilePlotData = {
  fileCountValues: string[];
  fileCountMap: Map<number, number>;
}

export type ContainerPlotData = {
  containerCountValues: string[];
  containerCountMap: Map<number, number>;
}

export type InsightsState = {
  volumeBucketMap: Map<string, Set<string>>;
  volumeOptions: Option[];
  fileCountError: string | undefined;
  containerSizeError: string | undefined;
}

//-------------------------//
//---OM DB Insights types---
//-------------------------//
type ReplicationConfig = {
  replicationFactor: string;
  requiredNodes: number;
  replicationType: string;
}

export type Pipelines = {
  id: {
    id: string;
  },
  replicationConfig: ReplicationConfig;
  healthy: boolean;
}

// Container Mismatch Info
export type Container = {
  containerId: number;
  numberOfKeys: number;
  pipelines: Pipelines[];
  existsAt: 'OM' | 'SCM';
}

export type MismatchContainersResponse = {
  containerDiscrepancyInfo: Container[];
}

// Deleted Container Keys
export type DeletedContainerKeysResponse = {
  containers: Container[];
}

export type MismatchKeys = {
  Volume: string;
  Bucket: string;
  Key: string;
  DataSize: number;
  Versions: number[];
  Blocks: Record<string, []>
  CreationTime: string;
  ModificationTime: string;
}

export type MismatchKeysResponse = {
  totalCount: number;
  keys: MismatchKeys[];
}

export interface RatisInfo {
  replicationType: 'RATIS';
  replicationFactor: string;
  requiredNodes: number;
  minimumNodes: number;
}

export interface EcInfo {
  replicationType: 'EC';
  data: number;
  parity: number;
  ecChunkSize: number;
  codec: string;
  requiredNodes: number;
  minimumNodes: number;
}

export type ReplicationInfo = RatisInfo | EcInfo;

// Open Keys
export type OpenKeys = {
  key: string;
  path: string;
  inStateSince: number;
  size: number;
  replicatedSize: number;
  replicationInfo: ReplicationInfo;
  creationTime: number;
  modificationTime: number;
  isKey: boolean;
}

export type OpenKeysResponse = {
  lastKey: string;
  replicatedDataSize: number;
  unreplicatedDataSize: number;
  fso?: OpenKeys[];
  nonFSO?: OpenKeys[];
}

//Keys pending deletion
export type DeletePendingKey = {
  objectID: number;
  updateID: number;
  parentObjectID: number;
  volumeName: string;
  bucketName: string;
  keyName: string;
  dataSize: number;
  creationTime: number;
  modificationTime: number;
  replicationConfig: ReplicationConfig;
  fileChecksum: number | null;
  fileName: string;
  file: boolean;
  path: string;
  hsync: boolean;
  replicatedSize: number;
  fileEncryptionInfo: string | null;
  objectInfo: string;
  updateIDSet: boolean;
}

export type DeletePendingKeysResponse = {
  lastKey: string;
  keysSummary: {
    totalUnreplicatedDataSize: number,
    totalReplicatedDataSize: number,
    totalDeletedKeys: number
  },
  replicatedDataSize: number;
  unreplicatedDataSize: number;
  deletedKeyInfo: {
    omKeyInfoList: DeletePendingKey[]
  }[];
}

//Directories Pending for Deletion
export type DeletedDirInfo = {
  key: string;
  path: string;
  inStateSince: number;
  size: number;
  replicatedSize: number;
  replicationInfo: ReplicationConfig;
  creationTime: number;
  modificationTime: number;
  isKey: boolean;
}

export type DeletedDirReponse = {
  lastKey: string;
  replicatedDataSize: number;
  unreplicatedDataSize: number;
  deletedDirInfo: DeletedDirInfo[];
  status: string;
}

export type ExpandedRow = {
  [key: number]: ExpandedRowState;
}

export type ExpandedRowState = {
  containerId: number;
  dataSource: MismatchKeys[];
  totalCount: number;
}
