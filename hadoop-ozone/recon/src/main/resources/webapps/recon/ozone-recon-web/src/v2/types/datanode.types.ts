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

import { Pipeline } from "@/v2/types/pipelines.types";
import { StorageReport } from "@/v2/types/overview.types";
import { Option as MultiOption } from "@/v2/components/select/multiSelect";

// Corresponds to HddsProtos.NodeState
export const DatanodeStateList = ['HEALTHY', 'STALE', 'DEAD'] as const;
type DatanodeStateType = typeof DatanodeStateList;
export type DatanodeState = DatanodeStateType[number];

// Corresponds to HddsProtos.NodeOperationalState
export const DatanodeOpStateList = [
  'IN_SERVICE',
  'DECOMMISSIONING',
  'DECOMMISSIONED',
  'ENTERING_MAINTENANCE',
  'IN_MAINTENANCE'
] as const;
export type DatanodeOpState = typeof DatanodeOpStateList[number];

export type DatanodeResponse = {
  hostname: string;
  state: DatanodeState;
  opState: DatanodeOpState;
  lastHeartbeat: string;
  storageReport: StorageReport;
  pipelines: Pipeline[];
  containers: number;
  openContainers: number;
  leaderCount: number;
  uuid: string;
  version: string;
  setupTime: number;
  revision: string;
  buildDate: string;
  networkLocation: string;
}

export type DatanodesResponse = {
  totalCount: number;
  datanodes: DatanodeResponse[];
}

export type Datanode = {
  hostname: string;
  state: DatanodeState;
  opState: DatanodeOpState;
  lastHeartbeat: string;
  storageUsed: number;
  storageTotal: number;
  storageRemaining: number;
  storageCommitted: number;
  pipelines: Pipeline[];
  containers: number;
  openContainers: number;
  leaderCount: number;
  uuid: string;
  version: string;
  setupTime: number;
  revision: string;
  buildDate: string;
  networkLocation: string;
}

export type DatanodeDetails = {
  uuid: string;
}

export type DatanodeDecomissionInfo = {
  datanodeDetails: DatanodeDetails
}

export type DatanodesState = {
  dataSource: Datanode[];
  lastUpdated: number;
  columnOptions: MultiOption[];
}

// Datanode Summary endpoint types
type summaryByteString = {
  string: string;
  bytes: {
    validUtf8: boolean;
    empty: boolean;
  }
}

type SummaryPort = {
  name: string;
  value: number;
}

type SummaryDatanodeDetails = {
  level: number;
  parent: unknown | null;
  cost: number;
  uuid: string;
  uuidString: string;
  ipAddress: string;
  hostName: string;
  ports: SummaryPort;
  certSerialId: null,
  version: string | null;
  setupTime: number;
  revision: string | null;
  buildDate: string;
  persistedOpState: string;
  persistedOpStateExpiryEpochSec: number;
  initialVersion: number;
  currentVersion: number;
  decommissioned: boolean;
  maintenance: boolean;
  ipAddressAsByteString: summaryByteString;
  hostNameAsByteString: summaryByteString;
  networkName: string;
  networkLocation: string;
  networkFullPath: string;
  numOfLeaves: number;
  networkNameAsByteString: summaryByteString;
  networkLocationAsByteString: summaryByteString
}

type SummaryMetrics = {
  decommissionStartTime: string;
  numOfUnclosedPipelines: number;
  numOfUnderReplicatedContainers: number;
  numOfUnclosedContainers: number;
}

type SummaryContainers = {
  UnderReplicated: string[];
  UnClosed: string[];
}

export type SummaryData = {
  datanodeDetails: SummaryDatanodeDetails;
  metrics: SummaryMetrics;
  containers: SummaryContainers;
}

export type DatanodeTableProps = {
  loading: boolean;
  selectedRows: React.Key[];
  data: Datanode[];
  decommissionUuids: string | string[];
  searchColumn: 'hostname' | 'uuid' | 'version' | 'revision';
  searchTerm: string;
  selectedColumns: MultiOption[];
  handleSelectionChange: (arg0: React.Key[]) => void;
}
