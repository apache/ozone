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
import { AxiosError } from 'axios';
import {
  Table,
  Tooltip,
  Popover,
  Button,
  Modal
} from 'antd';
import {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import {
  CheckCircleFilled,
  CloseCircleFilled,
  DeleteOutlined,
  HourglassFilled,
  InfoCircleOutlined,
  WarningFilled,
} from '@ant-design/icons';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import StorageBar from '@/v2/components/storageBar/storageBar';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import { showDataFetchError } from '@/utils/common';
import { ReplicationIcon } from '@/utils/themeIcons';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import {
  AxiosGetHelper,
  AxiosPutHelper,
  cancelRequests
} from '@/utils/axiosRequestHelper';

import { useDebounce } from '@/v2/hooks/debounce.hook';
import {
  Datanode,
  DatanodeDecomissionInfo,
  DatanodeOpState,
  DatanodeOpStateList,
  DatanodeResponse,
  DatanodesResponse,
  DatanodesState,
  DatanodeState,
  DatanodeStateList
} from '@/v2/types/datanode.types';
import { Pipeline } from '@/v2/types/pipelines.types';

import './datanodes.less'
import DecommissionSummary from '@/v2/components/decommissioningSummary/decommissioningSummary';
import { ColumnTitleProps } from 'antd/lib/table/interface';
import { TableRowSelection } from 'antd/es/table/interface';

moment.updateLocale('en', {
  relativeTime: {
    past: '%s ago',
    s: '%ds',
    m: '1min',
    mm: '%dmins',
    h: '1hr',
    hh: '%dhrs',
    d: '1d',
    dd: '%dd',
    M: '1m',
    MM: '%dm',
    y: '1y',
    yy: '%dy'
  }
});

const headerIconStyles: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center'
}

const renderDatanodeState = (state: DatanodeState) => {
  const stateIconMap = {
    HEALTHY: <CheckCircleFilled twoToneColor='#1da57a' className='icon-success' />,
    STALE: <HourglassFilled className='icon-warning' />,
    DEAD: <CloseCircleFilled className='icon-failure' />
  };
  const icon = state in stateIconMap ? stateIconMap[state] : '';
  return <span>{icon} {state}</span>;
};

const renderDatanodeOpState = (opState: DatanodeOpState) => {
  const opStateIconMap = {
    IN_SERVICE: <CheckCircleFilled twoToneColor='#1da57a' className='icon-success' />,
    DECOMMISSIONING: <HourglassFilled className='icon-warning' />,
    DECOMMISSIONED: <WarningFilled className='icon-warning' />,
    ENTERING_MAINTENANCE: <HourglassFilled className='icon-warning' />,
    IN_MAINTENANCE: <WarningFilled className='icon-warning' />
  };
  const icon = opState in opStateIconMap ? opStateIconMap[opState] : '';
  return <span>{icon} {opState}</span>;
};

const getTimeDiffFromTimestamp = (timestamp: number): string => {
  const timestampDate = new Date(timestamp);
  return moment(timestampDate).fromNow();
}

const COLUMNS: ColumnsType<Datanode> = [
  {
    title: 'Hostname',
    dataIndex: 'hostname',
    key: 'hostname',
    sorter: (a: Datanode, b: Datanode) => a.hostname.localeCompare(
      b.hostname, undefined, { numeric: true }
    ),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'State',
    dataIndex: 'state',
    key: 'state',
    filterMultiple: true,
    filters: DatanodeStateList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Datanode) => record.state === value,
    render: (text: DatanodeState) => renderDatanodeState(text),
    sorter: (a: Datanode, b: Datanode) => a.state.localeCompare(b.state)
  },
  {
    title: 'Operational State',
    dataIndex: 'opState',
    key: 'opState',
    filterMultiple: true,
    filters: DatanodeOpStateList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Datanode) => record.opState === value,
    render: (text: DatanodeOpState) => renderDatanodeOpState(text),
    sorter: (a: Datanode, b: Datanode) => a.opState.localeCompare(b.opState)
  },
  {
    title: 'UUID',
    dataIndex: 'uuid',
    key: 'uuid',
    sorter: (a: Datanode, b: Datanode) => a.uuid.localeCompare(b.uuid),
    defaultSortOrder: 'ascend' as const,
    render: (uuid: string, record: Datanode) => {
      return (
        //1. Compare Decommission Api's UUID with all UUID in table and show Decommission Summary
        (decommissionUuids && decommissionUuids.includes(record.uuid) && record.opState !== 'DECOMMISSIONED') ?
          <DecommissionSummary uuid={uuid} /> : <span>{uuid}</span>
      );
    }
  },
  {
    title: 'Storage Capacity',
    dataIndex: 'storageUsed',
    key: 'storageUsed',
    sorter: (a: Datanode, b: Datanode) => a.storageRemaining - b.storageRemaining,
    render: (_: string, record: Datanode) => (
      <StorageBar
        strokeWidth={6}
        capacity={record.storageTotal}
        used={record.storageUsed}
        remaining={record.storageRemaining}
        committed={record.storageCommitted} />
    )
  },
  {
    title: 'Last Heartbeat',
    dataIndex: 'lastHeartbeat',
    key: 'lastHeartbeat',
    sorter: (a: Datanode, b: Datanode) => moment(a.lastHeartbeat).unix() - moment(b.lastHeartbeat).unix(),
    render: (heartbeat: number) => {
      return heartbeat > 0 ? getTimeDiffFromTimestamp(heartbeat) : 'NA';
    }
  },
  {
    title: 'Pipeline ID(s)',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: Pipeline[], record: Datanode) => {
      const renderPipelineIds = (pipelineIds: Pipeline[]) => {
        return pipelineIds?.map((pipeline: any, index: any) => (
          <div key={index} className='pipeline-container-v2'>
            <ReplicationIcon
              replicationFactor={pipeline.replicationFactor}
              replicationType={pipeline.replicationType}
              leaderNode={pipeline.leaderNode}
              isLeader={pipeline.leaderNode === record.hostname} />
            {pipeline.pipelineID}
          </div >
        ))
      }

      return (
        <Popover
          content={
            renderPipelineIds(pipelines)
          }
          title="Related Pipelines"
          placement="bottomLeft"
          trigger="hover">
          <strong>{pipelines.length}</strong> pipelines
        </Popover>
      );
    }
  },
  {
    title: () => (
      <span style={headerIconStyles} >
        Leader Count
        <Tooltip
          title='The number of Ratis Pipelines in which the given datanode is elected as a leader.' >
          <InfoCircleOutlined style={{ paddingLeft: '4px' }} />
        </Tooltip>
      </span>
    ),
    dataIndex: 'leaderCount',
    key: 'leaderCount',
    sorter: (a: Datanode, b: Datanode) => a.leaderCount - b.leaderCount
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers',
    sorter: (a: Datanode, b: Datanode) => a.containers - b.containers
  },
  {
    title: () => (
      <span style={headerIconStyles}>
        Open Container
        <Tooltip title='The number of open containers per pipeline.'>
          <InfoCircleOutlined style={{ paddingLeft: '4px' }} />
        </Tooltip>
      </span>
    ),
    dataIndex: 'openContainers',
    key: 'openContainers',
    sorter: (a: Datanode, b: Datanode) => a.openContainers - b.openContainers
  },
  {
    title: 'Version',
    dataIndex: 'version',
    key: 'version',
    sorter: (a: Datanode, b: Datanode) => a.version.localeCompare(b.version),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Setup Time',
    dataIndex: 'setupTime',
    key: 'setupTime',
    sorter: (a: Datanode, b: Datanode) => a.setupTime - b.setupTime,
    render: (uptime: number) => {
      return uptime > 0 ? moment(uptime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Revision',
    dataIndex: 'revision',
    key: 'revision',
    sorter: (a: Datanode, b: Datanode) => a.revision.localeCompare(b.revision),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Build Date',
    dataIndex: 'buildDate',
    key: 'buildDate',
    sorter: (a: Datanode, b: Datanode) => a.buildDate.localeCompare(b.buildDate),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Network Location',
    dataIndex: 'networkLocation',
    key: 'networkLocation',
    sorter: (a: Datanode, b: Datanode) => a.networkLocation.localeCompare(b.networkLocation),
    defaultSortOrder: 'ascend' as const
  }
];

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
}, {
  label: 'Revision',
  value: 'revision'
}];

let decommissionUuids: string | string[] = [];
const COLUMN_UPDATE_DECOMMISSIONING = 'DECOMMISSIONING';

const Datanodes: React.FC<{}> = () => {

  const cancelSignal = useRef<AbortController>();
  const cancelDecommissionSignal = useRef<AbortController>();

  const [state, setState] = useState<DatanodesState>({
    lastUpdated: 0,
    columnOptions: defaultColumns,
    dataSource: []
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedRows, setSelectedRows] = useState<React.Key[]>([]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [searchColumn, setSearchColumn] = useState<'hostname' | 'uuid' | 'version' | 'revision'>('hostname');
  const [modalOpen, setModalOpen] = useState<boolean>(false);

  const debouncedSearch = useDebounce(searchTerm, 300);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    );
  }

  function getFilteredData(data: Datanode[]) {
    return data.filter(
      (datanode: Datanode) => datanode[searchColumn].includes(debouncedSearch)
    );
  }

  async function loadDecommisionAPI() {
    decommissionUuids = [];
    const { request, controller } = await AxiosGetHelper(
      '/api/v1/datanodes/decommission/info',
      cancelDecommissionSignal.current
    );
    cancelDecommissionSignal.current = controller;
    return request
  };

  async function loadDataNodeAPI() {
    const { request, controller } = await AxiosGetHelper(
      '/api/v1/datanodes',
      cancelSignal.current
    );
    cancelSignal.current = controller;
    return request;
  };

  async function removeDatanode(selectedRowKeys: string[]) {
    setLoading(true);
    const { request, controller } = await AxiosPutHelper(
      '/api/v1/datanodes/remove',
      selectedRowKeys,
      cancelSignal.current
    );
    cancelSignal.current = controller;
    request.then(() => {
      loadData();
    }).catch((error) => {
      showDataFetchError(error.toString());
    }).finally(() => {
      setLoading(false);
      setSelectedRows([]);
    });
  }

  const loadData = async () => {
    setLoading(true);
    // Need to call decommission API on each interval to get updated status
    // before datanode API call to compare UUID's
    // update 'Operation State' column in table manually before rendering
    try {
      let decomissionResponse = await loadDecommisionAPI();
      decommissionUuids = decomissionResponse.data?.DatanodesDecommissionInfo?.map(
        (item: DatanodeDecomissionInfo) => item.datanodeDetails.uuid
      );
    } catch (error) {
      decommissionUuids = [];
      showDataFetchError((error as AxiosError).toString());
    }

    try {
      const datanodesAPIResponse = await loadDataNodeAPI();
      const datanodesResponse: DatanodesResponse = datanodesAPIResponse.data;
      const datanodes: DatanodeResponse[] = datanodesResponse.datanodes;
      const dataSource: Datanode[] = datanodes?.map(
        (datanode) => ({
          hostname: datanode.hostname,
          uuid: datanode.uuid,
          state: datanode.state,
          opState: (decommissionUuids?.includes(datanode.uuid) && datanode.opState !== 'DECOMMISSIONED')
            ? COLUMN_UPDATE_DECOMMISSIONING
            : datanode.opState,
          lastHeartbeat: datanode.lastHeartbeat,
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
      setLoading(false);
      setState({
        ...state,
        dataSource: dataSource,
        lastUpdated: Number(moment())
      });
    } catch (error) {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString())
    }
  }

  const autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  useEffect(() => {
    autoReloadHelper.startPolling();
    loadData();

    return (() => {
      autoReloadHelper.stopPolling();
      cancelRequests([
        cancelSignal.current!,
        cancelDecommissionSignal.current!
      ]);
    });
  }, []);

  function isSelectable(record: Datanode) {
    // Disable checkbox for any datanode which is not DEAD to prevent removal
    return record.state !== 'DEAD' && true;
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

  const rowSelection: TableRowSelection<Datanode> = {
    selectedRowKeys: selectedRows,
    onChange: (rows: React.Key[]) => { setSelectedRows(rows) },
    getCheckboxProps: (record: Datanode) => ({
      disabled: isSelectable(record)
    }),
  };

  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => (
      `${range[0]}-${range[1]} of ${total} Datanodes`
    ),
    showSizeChanger: true
  };
  console.log(selectedRows);

  return (
    <>
      <div className='page-header-v2'>
        Datanodes
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
                fixedColumn='hostname'
                columnLength={columnOptions.length} />
              {selectedRows.length > 0 &&
                <Button
                  type="primary"
                  icon={<DeleteOutlined />}
                  style={{
                    background: '#FF4D4E',
                    borderColor: '#FF4D4E'
                  }}
                  loading={loading}
                  onClick={() => { setModalOpen(true) }}> Remove
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
              }} />
          </div>
          <div>
            <Table
              rowSelection={rowSelection}
              dataSource={getFilteredData(dataSource)}
              columns={filterSelectedColumns()}
              loading={loading}
              rowKey='uuid'
              pagination={paginationConfig}
              scroll={{ x: 'max-content', y: 400, scrollToFirstRowOnChange: true }}
              locale={{ filterTitle: '' }} />
          </div>
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
            }}>
              <WarningFilled className='icon-warning' style={{paddingRight: '8px'}}/>
              Stop Tracking Datanode
            </div>
            Are you sure, you want recon to stop tracking the selected <strong>{selectedRows.length}</strong> datanode(s)?
        </Modal>
    </>
  );
}

export default Datanodes;