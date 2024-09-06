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

import React, {
  useEffect,
  useRef,
  useState
} from 'react';
import moment from 'moment';
import { Table, Tooltip } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import { ValueType } from 'react-select';
import prettyMilliseconds from 'pretty-ms';

import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import Search from '@/v2/components/search/search';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import { showDataFetchError } from '@/utils/common';
import { ReplicationIcon } from '@/utils/themeIcons';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';
import { useDebounce } from '@/v2/hooks/debounce.hook';

import {
  Pipeline,
  PipelinesResponse,
  PipelinesState,
  PipelineStatusList
} from '@/v2/types/pipelines.types';

import './pipelines.less';
import { getDurationFromTimestamp, getTimeDiffFromTimestamp } from '@/v2/utils/momentUtils';

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

const COLUMNS: ColumnsType<Pipeline> = [
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

const defaultColumns = COLUMNS.map(column => ({
  label: (typeof column.title === 'string')
    ? column.title
    : (column.title as Function)().props.children[0],
  value: column.key as string,
}));

const Pipelines: React.FC<{}> = () => {
  const cancelSignal = useRef<AbortController>();

  const [state, setState] = useState<PipelinesState>({
    activeDataSource: [],
    columnOptions: defaultColumns,
    lastUpdated: 0,
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [searchTerm, setSearchTerm] = useState<string>('');

  const debouncedSearch = useDebounce(searchTerm, 300);

  const loadData = () => {
    setLoading(true);
    //Cancel any previous requests
    cancelRequests([cancelSignal.current!]);

    const { request, controller } = AxiosGetHelper(
      '/api/v1/pipelines',
      cancelSignal.current
    );

    cancelSignal.current = controller;
    request.then(response => {
      const pipelinesResponse: PipelinesResponse = response.data;
      const pipelines: Pipeline[] = pipelinesResponse?.pipelines ?? {};
      setState({
        ...state,
        activeDataSource: pipelines,
        lastUpdated: Number(moment())
      })
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError(error.toString());
    })
  }

  const autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  useEffect(() => {
    autoReloadHelper.startPolling();
    loadData();
    return (() => {
      autoReloadHelper.stopPolling();
      cancelRequests([cancelSignal.current!]);
    })
  }, []);

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    )
  }

  function getFilteredData(data: Pipeline[]) {
    return data.filter(
      (pipeline: Pipeline) => pipeline['pipelineId'].includes(debouncedSearch)
    )
  }

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  const {
    activeDataSource,
    columnOptions,
    lastUpdated
  } = state;
  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} pipelines`,
    showSizeChanger: true,
  };
  return (
    <>
      <div className='page-header-v2'>
        Pipelines
        <AutoReloadPanel
          isLoading={loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReloadHelper.handleAutoReloadToggle}
          onReload={loadData} />
      </div>
      <div style={{ padding: '24px' }}>
        <div className='content-div'>
          <div className='table-header-section'>
            <div className='table-filter-section'>
              <MultiSelect
                options={columnOptions}
                defaultValue={selectedColumns}
                selected={selectedColumns}
                placeholder='Columns'
                onChange={handleColumnChange}
                onTagClose={() => { }}
                fixedColumn='pipelineId'
                columnLength={COLUMNS.length} />
            </div>
            <Search
              disabled={activeDataSource?.length < 1}
              searchOptions={[{
                label: 'Pipeline ID',
                value: 'pipelineId'
              }]}
              searchInput={searchTerm}
              searchColumn={'pipelineId'}
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={() => { }} />
          </div>
          <div>
            <Table
              dataSource={getFilteredData(activeDataSource)}
              columns={filterSelectedColumns()}
              loading={loading}
              rowKey='pipelineId'
              pagination={paginationConfig}
              scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
              locale={{ filterTitle: '' }} />
          </div>
        </div>
      </div>
    </>
  );
}
export default Pipelines;