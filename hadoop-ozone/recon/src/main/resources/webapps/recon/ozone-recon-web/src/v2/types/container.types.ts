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

export type ContainerReplica = {
  containerId: number;
  datanodeUuid: string;
  datanodeHost: string;
  firstSeenTime: number;
  lastSeenTime: number;
  lastBcsId: number;
  dataChecksum?: string;
}

export type Container = {
  containerID: number;
  containerState: string;
  unhealthySince: number;
  expectedReplicaCount: number;
  actualReplicaCount: number;
  replicaDeltaCount: number;
  reason: string;
  keys: number;
  pipelineID: string;
  replicas: ContainerReplica[];
}

type KeyResponseBlock = {
  containerID: number;
  localID: number;
}

export type KeyResponse = {
  Volume: string;
  Bucket: string;
  Key: string;
  DataSize: number;
  CompletePath: string;
  Versions: number[];
  Blocks: Record<number, KeyResponseBlock[]>;
  CreationTime: string;
  ModificationTime: string;
}

export type ContainerKeysResponse = {
  totalCount: number;
  keys: KeyResponse[];
}

export type ContainerTableProps = {
  loading: boolean;
  data: Container[];
  searchColumn: 'containerID' | 'pipelineID';
  searchTerm: string;
  selectedColumns: Option[];
  expandedRow: ExpandedRow;
  expandedRowSetter: (arg0: ExpandedRow) => void;
}


export type ExpandedRow = {
  [key: number]: ExpandedRowState;
}

export type ExpandedRowState = {
  loading: boolean;
  containerId: number;
  dataSource: KeyResponse[];
  totalCount: number;
}

export type ContainerState = {
  lastUpdated: number;
  columnOptions: Option[];
  missingContainerData: Container[];
  underReplicatedContainerData: Container[];
  overReplicatedContainerData: Container[];
  misReplicatedContainerData: Container[];
  mismatchedReplicaContainerData: Container[];
}