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

export const overviewLocators = {
  'datanodeRow': 'overview-Health-Datanodes',
  'containersRow': 'overview-Health-Containers',
  'capacityOzoneUsed': 'capacity-ozone-used',
  'capacityNonOzoneUsed': 'capacity-non-ozone-used',
  'capacityRemaining': 'capacity-remaining',
  'capacityPreAllocated': 'capacity-pre-allocated',
  'volumesCard': 'overview-Volumes',
  'bucketsCard': 'overview-Buckets',
  'keysCard': 'overview-Keys',
  'pipelinesCard': 'overview-Pipelines',
  'deletedContainersCard': 'overview-Deleted Containers',
  'openTotalReplicatedData': 'overview-Open Keys Summary-Total Replicated Data',
  'openTotalUnreplicatedData': 'overview-Open Keys Summary-Total Unreplicated Data',
  'openKeys': 'overview-Open Keys Summary-Open Keys',
  'deletePendingTotalReplicatedData': 'overview-Delete Pending Keys Summary-Total Replicated Data',
  'deletePendingTotalUnreplicatedData': 'overview-Delete Pending Keys Summary-Total Unreplicated Data',
  'deletePendingKeys': 'overview-Delete Pending Keys Summary-Delete Pending Keys'
}

export const datanodeLocators = {
  'datanodeMultiSelect': 'dn-multi-select',
  'datanodeSearchcDropdown': 'search-dropdown',
  'datanodeSearchInput': 'search-input',
  'datanodeRemoveButton': 'dn-remove-btn',
  'datanodeRemoveModal': 'dn-remove-modal',
  'datanodeTable': 'dn-table',
  'datanodeRowRegex': /dntable-/,
  datanodeSearchOption: (label: string) => `search-opt-${label}`,
  datanodeTableRow: (uuid: string) =>  `dntable-${uuid}`
}

export const pipelineLocators = {
  'pipelineTable': 'pipelines-table',
  'pipelineRowRegex': /pipelinetable-/,
  pipelineTableRow: (uuid: string) => `pipelinetable-${uuid}`
}

export const autoReloadPanelLocators = {
  'autoreloadPanel': 'autoreload-panel',
  'refreshButton': 'autoreload-panel-refresh',
  'toggleSwitch': 'autoreload-panel-switch'
}

export const searchInputLocator = 'search-input';