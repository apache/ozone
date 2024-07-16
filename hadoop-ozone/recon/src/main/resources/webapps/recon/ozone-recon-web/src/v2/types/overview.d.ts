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
import { RouteComponentProps } from "react-router-dom";

export interface IOverviewCardProps extends RouteComponentProps {
  icon: string;
  data: string | ReactElement;
  title: string;
  hoverable?: boolean;
  loading?: boolean;
  linkToUrl?: string;
  storageReport?: IStorageReport;
  error?: boolean;
}

export type OverviewCardWrapperProps = {
  linkToUrl: string;
  title: string;
  children: React.ReactElement;
}

export type ClusterStateResponse = {
  missingContainers: number;
  totalDatanodes: number;
  healthyDatanodes: number;
  pipelines: number;
  storageReport: IStorageReport;
  containers: number;
  volumes: number;
  buckets: number;
  keys: number;
  openContainers: number;
  deletedContainers: number;
  keysPendingDeletion: number;
  scmServiceId: string;
  omServiceId: string;
}

export type OverviewState = {
  loading: boolean;
  datanodes: string;
  pipelines: number | string;
  containers: number | string;
  volumes: number | string;
  buckets: number | string;
  keys: number | string;
  missingContainersCount: number | string;
  lastRefreshed: number | string;
  lastUpdatedOMDBDelta: number | string;
  lastUpdatedOMDBFull: number | string;
  omStatus: string;
  openContainers: number | string;
  deletedContainers: number | string;
  openSummarytotalUnrepSize: number | string;
  openSummarytotalRepSize: number | string;
  openSummarytotalOpenKeys: number | string;
  deletePendingSummarytotalUnrepSize: number | string;
  deletePendingSummarytotalRepSize: number | string;
  deletePendingSummarytotalDeletedKeys: number | string;
  scmServiceId: string;
  omServiceId: string;
}

export type IStorageReport = {
  capacity: number;
  used: number;
  remaining: number;
  committed: number;
}
