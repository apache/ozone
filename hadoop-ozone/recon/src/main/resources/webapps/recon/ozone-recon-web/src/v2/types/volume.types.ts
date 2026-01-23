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
import { Option } from "@/v2/components/select/multiSelect";

export type Volume = {
  volume: string;
  owner: string;
  admin: string;
  creationTime: number;
  modificationTime: number;
  quotaInBytes: number;
  quotaInNamespace: number;
  usedNamespace: number;
  acls?: Acl[];
}

export type VolumesResponse = {
  totalCount: number;
  volumes: Volume[];
}

export type VolumesState = {
  data: Volume[];
  lastUpdated: number;
  columnOptions: Option[];
}

export type VolumesTableProps = {
  loading: boolean;
  data: Volume[];
  handleAclClick: (arg0: Volume) => void;
  selectedColumns: Option[];
  searchColumn: 'volume' | 'owner' | 'admin';
  searchTerm: string;
}
