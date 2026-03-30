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
import {
  Button,
  Card,
  Modal,
  Table,
  Tag,
  Tooltip
} from 'antd';
import {
  DeleteOutlined,
  WarningFilled,
  ExclamationCircleOutlined,
} from '@ant-design/icons';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import DatanodesTable, { COLUMNS } from '@/v2/components/tables/datanodesTable';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import { showDataFetchError } from '@/utils/common';
import { useApiData } from '@/v2/hooks/useAPIData.hook';

import { useDebounce } from '@/v2/hooks/useDebounce';
import {
  Datanode,
  DatanodeDecomissionInfo,
  DatanodeResponse,
  DatanodesResponse,
  DatanodesState,
  DatanodeUnhealthySummary,
  DatanodeUnhealthyByDatanodeResponse
} from '@/v2/types/datanode.types';

import './datanodes.less'
import { useAutoReload } from '@/v2/hooks/useAutoReload.hook';

// Type for decommission API response
type DecommissionAPIResponse = {
  DatanodesDecommissionInfo: DatanodeDecomissionInfo[];
};

const defaultColumns = COLUMNS.map(column => ({
  label: (typeof column.title === 'string')
    ? column.title
    : (column.title as Function)().props.children[0],
  value: column.key as string
}));

const SearchableColumnOpts = [{
  label: 'Hostname',
  value: 'hostname'
}, {
  label: 'UUID',
  value: 'uuid'
}, {
  label: 'Version',
  value: 'version'
}];

let decommissionUuids: string | string[] = [];
const COLUMN_UPDATE_DECOMMISSIONING = 'DECOMMISSIONING';

const STATE_COLORS: Record<string, string> = {
  'MISSING': '#ff4d4f',
  'UNDER_REPLICATED': '#fa8c16',
  'OVER_REPLICATED': '#1890ff',
  'MIS_REPLICATED': '#722ed1',
  'REPLICA_MISMATCH': '#eb2f96'
};

const STATE_LABELS: Record<string, string> = {
  'MISSING': 'Missing',
  'UNDER_REPLICATED': 'Under-Replicated',
  'OVER_REPLICATED': 'Over-Replicated',
  'MIS_REPLICATED': 'Mis-Replicated',
  'REPLICA_MISMATCH': 'Mismatched'
};

const Datanodes: React.FC<{}> = () => {

  const [state, setState] = useState<DatanodesState>({
    lastUpdated: 0,
    columnOptions: defaultColumns,
    dataSource: []
  });
  
  // API hooks for data fetching
  const decommissionAPI = useApiData<DecommissionAPIResponse>(
    '/api/v1/datanodes/decommission/info',
    { DatanodesDecommissionInfo: [] },
    { 
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );
  
  const datanodesAPI = useApiData<DatanodesResponse>(
    '/api/v1/datanodes',
    { datanodes: [], totalCount: 0 },
    { 
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );
  
  const removeDatanodesAPI = useApiData<any>(
    '/api/v1/datanodes/remove',
    null,
    { 
      method: 'PUT',
      initialFetch: false,
      onError: (error) => showDataFetchError(error),
      onSuccess: () => {
        loadData();
        setSelectedRows([]);
      }
    }
  );

  // Unhealthy containers by DataNode API
  const unhealthyByDnAPI = useApiData<DatanodeUnhealthyByDatanodeResponse>(
    '/api/v1/containers/unhealthy/byDatanode',
    { datanodes: [] },
    {
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );
  
  const loading = decommissionAPI.loading || datanodesAPI.loading || removeDatanodesAPI.loading;
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedRows, setSelectedRows] = useState<React.Key[]>([]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [searchColumn, setSearchColumn] = useState<'hostname' | 'uuid' | 'version' | 'revision'>('hostname');
  const [modalOpen, setModalOpen] = useState<boolean>(false);

  const debouncedSearch = useDebounce(searchTerm, 300);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  async function removeDatanode(selectedRowKeys: string[]) {
    try {
      await removeDatanodesAPI.execute(selectedRowKeys);
    } catch (error) {
      showDataFetchError(error);
    }
  }

  const loadData = () => {
    // Trigger both API hooks to refetch data
    decommissionAPI.refetch();
    datanodesAPI.refetch();
    unhealthyByDnAPI.refetch();
  };

  // Process data when both APIs have loaded
  useEffect(() => {
    if (!decommissionAPI.loading && !datanodesAPI.loading && 
        decommissionAPI.data && datanodesAPI.data) {
      
      // Update decommission UUIDs
      decommissionUuids = decommissionAPI.data?.DatanodesDecommissionInfo?.map(
        (item: DatanodeDecomissionInfo) => item.datanodeDetails.uuid
      ) || [];

      const datanodes: DatanodeResponse[] = datanodesAPI.data.datanodes;
      const dataSource: Datanode[] = datanodes?.map(
        (datanode) => ({
          hostname: datanode.hostname,
          uuid: datanode.uuid,
          state: datanode.state,
          opState: (decommissionUuids?.includes(datanode.uuid) && datanode.opState !== 'DECOMMISSIONED')
            ? COLUMN_UPDATE_DECOMMISSIONING
            : datanode.opState,
          lastHeartbeat: datanode.lastHeartbeat,
          storageReport: datanode.storageReport,
          storageUsed: datanode.storageReport.used,
          storageTotal: datanode.storageReport.capacity,
          storageCommitted: datanode.storageReport.committed,
          storageRemaining: datanode.storageReport.remaining,
          pipelines: datanode.pipelines,
          containers: datanode.containers,
          openContainers: datanode.openContainers,
          leaderCount: datanode.leaderCount,
          version: datanode.version,
          setupTime: datanode.setupTime,
          revision: datanode.revision,
          buildDate: datanode.buildDate,
          networkLocation: datanode.networkLocation
        })
      );

      setState({
        ...state,
        dataSource: dataSource,
        lastUpdated: Number(moment())
      });
    }
  }, [decommissionAPI.loading, datanodesAPI.loading, decommissionAPI.data, datanodesAPI.data]);

  const autoReload = useAutoReload(loadData);

  useEffect(() => {
    autoReload.startPolling();

    return (() => {
      autoReload.stopPolling();
    });
  }, []);

  function handleSelectionChange(rows: React.Key[]) {
    setSelectedRows(rows);
  }

  function handleModalOk() {
    setModalOpen(false);
    removeDatanode(selectedRows as string[])
  };

  function handleModalCancel() {
    setModalOpen(false);
    setSelectedRows([]);
  };

  const { dataSource, lastUpdated, columnOptions } = state;

  // Columns for the unhealthy containers by DataNode table
  const unhealthyDnColumns = [
    {
      title: 'DataNode Host',
      dataIndex: 'datanodeHost',
      key: 'datanodeHost',
      sorter: (a: DatanodeUnhealthySummary, b: DatanodeUnhealthySummary) =>
        a.datanodeHost.localeCompare(b.datanodeHost),
    },
    {
      title: 'Total Unhealthy',
      dataIndex: 'totalUnhealthyContainers',
      key: 'totalUnhealthyContainers',
      defaultSortOrder: 'descend' as const,
      sorter: (a: DatanodeUnhealthySummary, b: DatanodeUnhealthySummary) =>
        a.totalUnhealthyContainers - b.totalUnhealthyContainers,
      render: (count: number) => (
        <span style={{ fontWeight: 600, color: count > 0 ? '#ff4d4f' : '#52c41a' }}>
          {count}
        </span>
      ),
    },
    ...Object.keys(STATE_LABELS).map(stateKey => ({
      title: STATE_LABELS[stateKey],
      key: stateKey,
      sorter: (a: DatanodeUnhealthySummary, b: DatanodeUnhealthySummary) =>
        (a.stateCounts[stateKey] ?? 0) - (b.stateCounts[stateKey] ?? 0),
      render: (_: any, record: DatanodeUnhealthySummary) => {
        const count = record.stateCounts[stateKey] ?? 0;
        return count > 0
          ? <Tag color={STATE_COLORS[stateKey]}>{count}</Tag>
          : <span style={{ color: '#bfbfbf' }}>0</span>;
      },
    })),
    {
      title: 'UUID',
      dataIndex: 'datanodeUuid',
      key: 'datanodeUuid',
      ellipsis: true,
      width: 180,
      render: (uuid: string) => (
        <Tooltip title={uuid}>
          <span style={{ fontFamily: 'monospace', fontSize: '12px' }}>
            {uuid.substring(0, 8)}…
          </span>
        </Tooltip>
      ),
    },
  ];

  const unhealthyDnData = unhealthyByDnAPI.data?.datanodes ?? [];

  return (
    <>
      <div className='page-header-v2'>
        Datanodes
        <AutoReloadPanel
          isLoading={loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReload.handleAutoReloadToggle}
          onReload={loadData} />
      </div>
      <div className='data-container'>
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
                fixedColumn='hostname'
                columnLength={columnOptions.length}
                data-testid='dn-multi-select' />
              {selectedRows.length > 0 &&
                <Button
                  type="primary"
                  icon={<DeleteOutlined />}
                  style={{
                    background: '#FF4D4E',
                    borderColor: '#FF4D4E'
                  }}
                  loading={loading}
                  onClick={() => { setModalOpen(true) }}
                  data-testid='dn-remove-btn'> Remove
                </Button>
              }
            </div>
            <Search
              disabled={dataSource?.length < 1}
              searchOptions={SearchableColumnOpts}
              searchInput={searchTerm}
              searchColumn={searchColumn}
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={(value) => {
                setSearchTerm('');
                setSearchColumn(value as 'hostname' | 'uuid' | 'version' | 'revision')
              }}/>
          </div>
          <DatanodesTable
            loading={loading}
            data={dataSource}
            selectedColumns={selectedColumns}
            selectedRows={selectedRows}
            searchColumn={searchColumn}
            searchTerm={debouncedSearch}
            handleSelectionChange={handleSelectionChange}
            decommissionUuids={decommissionUuids}/>
        </div>

        {/* Unhealthy Containers by DataNode Section */}
        <div style={{ padding: '24px 0 0 0' }}>
          <Card
            title={
              <span>
                <ExclamationCircleOutlined style={{ color: '#fa8c16', marginRight: 8 }} />
                Unhealthy Containers by DataNode
              </span>
            }
            loading={unhealthyByDnAPI.loading}
            extra={
              <span style={{ color: '#8c8c8c', fontSize: '13px' }}>
                {unhealthyDnData.length} DataNode(s) with unhealthy containers
              </span>
            }
          >
            {unhealthyDnData.length === 0 && !unhealthyByDnAPI.loading ? (
              <div style={{ textAlign: 'center', padding: '24px', color: '#8c8c8c' }}>
                No unhealthy containers found across DataNodes.
              </div>
            ) : (
              <Table
                dataSource={unhealthyDnData}
                columns={unhealthyDnColumns}
                rowKey='datanodeUuid'
                size='middle'
                pagination={{
                  pageSize: 20,
                  showSizeChanger: true,
                  showTotal: (total, range) =>
                    `${range[0]}-${range[1]} of ${total} DataNodes`
                }}
              />
            )}
          </Card>
        </div>

      </div>
      <Modal
          title=''
          centered={true}
          visible={modalOpen}
          onOk={handleModalOk}
          onCancel={handleModalCancel}
          closable={false}
          width={400} >
            <div style={{
              margin: '0px 0px 5px 0px',
              fontSize: '16px',
              fontWeight: 'bold'
            }}
          data-testid='dn-remove-modal'>
              <WarningFilled className='icon-warning' style={{paddingRight: '8px'}}/>
              Stop Tracking Datanode
            </div>
            Are you sure, you want recon to stop tracking the selected <strong>{selectedRows.length}</strong> datanode(s)?
        </Modal>
    </>
  );
}

export default Datanodes;