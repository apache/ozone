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

export const PipelineStatusList = [
  'OPEN',
  'CLOSING',
  'QUASI_CLOSED',
  'CLOSED',
  'UNHEALTHY',
  'INVALID',
  'DELETED',
  'DORMANT'
] as const;
export type PipelineStatus = typeof PipelineStatusList[number];

export type Pipeline = {
  pipelineId: string;
  status: PipelineStatus;
  replicationType: string;
  leaderNode: string;
  datanodes: string[];
  lastLeaderElection: number;
  duration: number;
  leaderElections: number;
  replicationFactor: string;
  containers: number;
}

export type PipelinesResponse = {
  totalCount: number;
  pipelines: Pipeline[];
}

export type PipelinesState = {
  activeDataSource: Pipeline[];
  columnOptions: Option[];
  lastUpdated: number;
}

export type PipelinesTableProps = {
  loading: boolean;
  data: Pipeline[];
  selectedColumns: Option[];
  searchTerm: string;
}
