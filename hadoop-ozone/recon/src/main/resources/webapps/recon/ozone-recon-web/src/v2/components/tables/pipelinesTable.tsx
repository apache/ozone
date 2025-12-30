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

import React, {HTMLAttributes} from 'react';

import Table, {ColumnsType, TablePaginationConfig} from 'antd/es/table';
import Tooltip from 'antd/es/tooltip';
import {InfoCircleOutlined} from '@ant-design/icons';

import {ReplicationIcon} from '@/utils/themeIcons';
import {getDurationFromTimestamp, getTimeDiffFromTimestamp} from '@/v2/utils/momentUtils';
import {Pipeline, PipelinesTableProps, PipelineStatusList} from '@/v2/types/pipelines.types';


// TODO: When Datanodes PR gets merged remove these declarations
// And import from datanodes.types

type SummaryDatanodeDetails = {
  level: number;
  parent: unknown | null;
  cost: number;
  uuid: string;
  uuidString: string;
  ipAddress: string;
  hostName: string;
  ports: {
    name: string;
    value: number
  }[];
  certSerialId: null,
  version: string | null;
  setupTime: number;
  revision: string | null;
  buildDate: string;
  persistedOpState: string;
  persistedOpStateExpiryEpochSec: number;
  initialVersion: number;
  currentVersion: number;
  signature: number;
  decommissioned: boolean;
  networkName: string;
  networkLocation: string;
  networkFullPath: string;
  numOfLeaves: number;
}

export const COLUMNS: ColumnsType<Pipeline> = [
  {
    title: 'Pipeline ID',
    dataIndex: 'pipelineId',
    key: 'pipelineId',
    sorter: (a: Pipeline, b: Pipeline) => a.pipelineId.localeCompare(b.pipelineId),

  },
  {
    title: 'Replication Type & Factor',
    dataIndex: 'replicationType',
    key: 'replicationType',
    render: (replicationType: string, record: Pipeline) => {
      const replicationFactor = record.replicationFactor;
      return (
        <span>
          <ReplicationIcon
            replicationFactor={replicationFactor}
            replicationType={replicationType}
            leaderNode={record.leaderNode}
            isLeader={false} />
          {replicationType} ({replicationFactor})
        </span>
      );
    },
    sorter: (a: Pipeline, b: Pipeline) =>
      (a.replicationType + a.replicationFactor.toString()).localeCompare(b.replicationType + b.replicationFactor.toString()),
    defaultSortOrder: 'descend' as const
  },
  {
    title: 'Status',
    dataIndex: 'status',
    key: 'status',
    filterMultiple: true,
    filters: PipelineStatusList.map(status => ({ text: status, value: status })),
    onFilter: (value, record: Pipeline) => record.status === value,
    sorter: (a: Pipeline, b: Pipeline) => a.status.localeCompare(b.status)
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers',
    sorter: (a: Pipeline, b: Pipeline) => a.containers - b.containers
  },
  {
    title: 'Datanodes',
    dataIndex: 'datanodes',
    key: 'datanodes',
    render: (datanodes: SummaryDatanodeDetails[]) => (
      <div>
        {datanodes.map(datanode => (
          <div className='uuid-tooltip'>
            <Tooltip
              placement='top'
              title={`UUID: ${datanode?.uuid ?? 'NA'}`}
              getPopupContainer={(triggerNode) => triggerNode}>
              {datanode?.hostName ?? 'N/A'}
            </Tooltip>
          </div>
        ))}
      </div>
    )
  },
  {
    title: 'Leader',
    dataIndex: 'leaderNode',
    key: 'leaderNode',
    sorter: (a: Pipeline, b: Pipeline) => a.leaderNode.localeCompare(b.leaderNode)
  },
  {
    title: () => (
      <span>
        Last Leader Election&nbsp;
        <Tooltip title='Elapsed time since the current leader got elected. Only available if any metrics service providers like Prometheus is configured.'>
          <InfoCircleOutlined />
        </Tooltip>
      </span>
    ),
    dataIndex: 'lastLeaderElection',
    key: 'lastLeaderElection',
    render: (lastLeaderElection: number) => lastLeaderElection > 0 ?
      getTimeDiffFromTimestamp(lastLeaderElection) : 'NA',
    sorter: (a: Pipeline, b: Pipeline) => a.lastLeaderElection - b.lastLeaderElection
  },
  {
    title: 'Lifetime',
    dataIndex: 'duration',
    key: 'duration',
    render: (duration: number) => getDurationFromTimestamp(duration),
    sorter: (a: Pipeline, b: Pipeline) => a.duration - b.duration
  },
  {
    title: () => (
      <span>
        No. of Elections&nbsp;
        <Tooltip title='Number of elections in this pipeline. Only available if any metrics service providers like Prometheus is configured.'>
          <InfoCircleOutlined />
        </Tooltip>
      </span>
    ),
    dataIndex: 'leaderElections',
    key: 'leaderElections',
    render: (leaderElections: number) => leaderElections > 0 ?
      leaderElections : 'NA',
    sorter: (a: Pipeline, b: Pipeline) => a.leaderElections - b.leaderElections
  }
];

const PipelinesTable: React.FC<PipelinesTableProps> = ({
  loading = false,
  data,
  selectedColumns,
  searchTerm = ''
}) => {
  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} pipelines`,
    showSizeChanger: true,
  };

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    )
  }

  function getFilteredData(data: Pipeline[]) {
    return data.filter(
      (pipeline: Pipeline) => pipeline['pipelineId'].includes(searchTerm)
    )
  }

  return (
    <div>
      <Table
        dataSource={getFilteredData(data)}
        columns={filterSelectedColumns()}
        loading={loading}
        rowKey='pipelineId'
        pagination={paginationConfig}
        scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
        locale={{ filterTitle: '' }}
        onRow={(record: Pipeline) => ({
          'data-testid': `pipelinetable-${record.pipelineId}`
        } as HTMLAttributes<HTMLElement>)}
        data-testid='pipelines-table'/>
    </div>
  )
}

export default PipelinesTable;
