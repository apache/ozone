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

import React from 'react';
import axios from 'axios';
import {Table, Icon, Tooltip} from 'antd';
import {PaginationConfig} from 'antd/lib/pagination';
import moment from 'moment';
import {ReplicationIcon} from 'utils/themeIcons';
import StorageBar from 'components/storageBar/storageBar';
import {
  DatanodeState,
  DatanodeStateList,
  DatanodeOpState,
  DatanodeOpStateList,
  IStorageReport
} from 'types/datanode.types';
import './datanodes.less';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';
import {MultiSelect, IOption} from 'components/multiSelect/multiSelect';
import {ActionMeta, ValueType} from 'react-select';
import {showDataFetchError} from 'utils/common';
import {ColumnSearch} from 'utils/columnSearch';

interface IDatanodeResponse {
  hostname: string;
  state: DatanodeState;
  opState: DatanodeOpState;
  lastHeartbeat: number;
  storageReport: IStorageReport;
  pipelines: IPipeline[];
  containers: number;
  leaderCount: number;
  uuid: string;
  version: string;
  setupTime: number;
  revision: string;
  buildDate: string;
}

interface IDatanodesResponse {
  totalCount: number;
  datanodes: IDatanodeResponse[];
}

interface IDatanode {
  hostname: string;
  state: DatanodeState;
  opState: DatanodeOpState;
  lastHeartbeat: number;
  storageUsed: number;
  storageTotal: number;
  storageRemaining: number;
  pipelines: IPipeline[];
  containers: number;
  leaderCount: number;
  uuid: string;
  version: string;
  setupTime: number;
  revision: string;
  buildDate: string;
}

interface IPipeline {
  pipelineID: string;
  replicationType: string;
  replicationFactor: number;
  leaderNode: string;
}

interface IDatanodesState {
  loading: boolean;
  dataSource: IDatanode[];
  totalCount: number;
  lastUpdated: number;
  selectedColumns: IOption[];
  columnOptions: IOption[];
}

const renderDatanodeState = (state: DatanodeState) => {
  const stateIconMap = {
    HEALTHY: <Icon type='check-circle' theme='filled' twoToneColor='#1da57a' className='icon-success'/>,
    STALE: <Icon type='hourglass' theme='filled' className='icon-warning'/>,
    DEAD: <Icon type='close-circle' theme='filled' className='icon-failure'/>
  };
  const icon = state in stateIconMap ? stateIconMap[state] : '';
  return <span>{icon} {state}</span>;
};

const renderDatanodeOpState = (opState: DatanodeOpState) => {
  const opStateIconMap = {
    IN_SERVICE: <Icon type='check-circle' theme='outlined' twoToneColor='#1da57a' className='icon-success'/>,
    DECOMMISSIONING: <Icon type='hourglass' theme='outlined' className='icon-warning'/>,
    DECOMMISSIONED: <Icon type='warning' theme='outlined' className='icon-warning'/>,
    ENTERING_MAINTENANCE: <Icon type='hourglass' theme='outlined' className='icon-warning'/>,
    IN_MAINTENANCE: <Icon type='warning' theme='outlined' className='icon-warning'/>
  };
  const icon = opState in opStateIconMap ? opStateIconMap[opState] : '';
  return <span>{icon} {opState}</span>;
};

const COLUMNS = [
  {
    title: 'State',
    dataIndex: 'state',
    key: 'state',
    isVisible: true,
    filterMultiple: true,
    filters: DatanodeStateList.map(state => ({text: state, value: state})),
    onFilter: (value: DatanodeState, record: IDatanode) => record.state === value,
    render: (text: DatanodeState) => renderDatanodeState(text),
    sorter: (a: IDatanode, b: IDatanode) => a.state.localeCompare(b.state),
    fixed: 'left'
  },
  {
    title: 'Operational State',
    dataIndex: 'opState',
    key: 'opState',
    isVisible: true,
    filterMultiple: true,
    filters: DatanodeOpStateList.map(state => ({text: state, value: state})),
    onFilter: (value: DatanodeOpState, record: IDatanode) => record.opState === value,
    render: (text: DatanodeOpState) => renderDatanodeOpState(text),
    sorter: (a: IDatanode, b: IDatanode) => a.state.localeCompare(b.state),
    fixed: 'left'
  },
  {
    title: 'Hostname',
    dataIndex: 'hostname',
    key: 'hostname',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IDatanode, b: IDatanode) => a.hostname.localeCompare(b.hostname),
    defaultSortOrder: 'ascend' as const,
    fixed: 'left'
  },
  {
    title: 'Uuid',
    dataIndex: 'uuid',
    key: 'uuid',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IDatanode, b: IDatanode) => a.uuid.localeCompare(b.uuid),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Storage Capacity',
    dataIndex: 'storageUsed',
    key: 'storageUsed',
    isVisible: true,
    sorter: (a: IDatanode, b: IDatanode) => a.storageRemaining - b.storageRemaining,
    render: (text: string, record: IDatanode) => (
      <StorageBar
        total={record.storageTotal} used={record.storageUsed}
        remaining={record.storageRemaining}/>
    )},
  {
    title: 'Last Heartbeat',
    dataIndex: 'lastHeartbeat',
    key: 'lastHeartbeat',
    isVisible: true,
    sorter: (a: IDatanode, b: IDatanode) => a.lastHeartbeat - b.lastHeartbeat,
    render: (heartbeat: number) => {
      return heartbeat > 0 ? moment(heartbeat).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Pipeline ID(s)',
    dataIndex: 'pipelines',
    key: 'pipelines',
    isVisible: true,
    render: (pipelines: IPipeline[], record: IDatanode) => {
      return (
        <div>
          {
            pipelines.map((pipeline, index) => (
              <div key={index} className='pipeline-container'>
                <ReplicationIcon
                  replicationFactor={pipeline.replicationFactor}
                  replicationType={pipeline.replicationType}
                  leaderNode={pipeline.leaderNode}
                  isLeader={pipeline.leaderNode === record.hostname}/>
                {pipeline.pipelineID}
              </div>
            ))
          }
        </div>
      );
    }
  },
  {
    title:
  <span>
    Leader Count&nbsp;
    <Tooltip title='The number of Ratis Pipelines in which the given datanode is elected as a leader.'>
      <Icon type='info-circle'/>
    </Tooltip>
  </span>,
    dataIndex: 'leaderCount',
    key: 'leaderCount',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IDatanode, b: IDatanode) => a.leaderCount - b.leaderCount
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IDatanode, b: IDatanode) => a.containers - b.containers
  },
  {
    title: 'Version',
    dataIndex: 'version',
    key: 'version',
    isVisible: false,
    isSearchable: true,
    sorter: (a: IDatanode, b: IDatanode) => a.version.localeCompare(b.version),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'SetupTime',
    dataIndex: 'setupTime',
    key: 'setupTime',
    isVisible: false,
    sorter: (a: IDatanode, b: IDatanode) => a.setupTime - b.setupTime,
    render: (uptime: number) => {
      return uptime > 0 ? moment(uptime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Revision',
    dataIndex: 'revision',
    key: 'revision',
    isVisible: false,
    isSearchable: true,
    sorter: (a: IDatanode, b: IDatanode) => a.revision.localeCompare(b.revision),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'BuildDate',
    dataIndex: 'buildDate',
    key: 'buildDate',
    isVisible: false,
    isSearchable: true,
    sorter: (a: IDatanode, b: IDatanode) => a.buildDate.localeCompare(b.buildDate),
    defaultSortOrder: 'ascend' as const
  }
];

const allColumnsOption: IOption = {
  label: 'Select all',
  value: '*'
};

const defaultColumns: IOption[] = COLUMNS.map(column => ({
  label: column.key,
  value: column.key
}));

export class Datanodes extends React.Component<Record<string, object>, IDatanodesState> {
  autoReload: AutoReloadHelper;

  constructor(props = {}) {
    super(props);
    this.state = {
      loading: false,
      dataSource: [],
      totalCount: 0,
      lastUpdated: 0,
      selectedColumns: [],
      columnOptions: defaultColumns
    };
    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _handleColumnChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const selectedColumns = (selected as IOption[]);
    this.setState({
      selectedColumns
    });
  };

  _getSelectedColumns = (selected: IOption[]) => {
    const selectedColumns = selected.length > 0 ? selected : COLUMNS.filter(column => column.isVisible).map(column => ({
      label: column.key,
      value: column.key
    }));
    return selectedColumns;
  };

  _loadData = () => {
    this.setState(prevState => ({
      loading: true,
      selectedColumns: this._getSelectedColumns(prevState.selectedColumns)
    }));
    axios.get('/api/v1/datanodes').then(response => {
      const datanodesResponse: IDatanodesResponse = response.data;
      const totalCount = datanodesResponse.totalCount;
      const datanodes: IDatanodeResponse[] = datanodesResponse.datanodes;
      const dataSource: IDatanode[] = datanodes.map(datanode => {
        return {
          hostname: datanode.hostname,
          uuid: datanode.uuid,
          state: datanode.state,
          opState: datanode.opState,
          lastHeartbeat: datanode.lastHeartbeat,
          storageUsed: datanode.storageReport.used,
          storageTotal: datanode.storageReport.capacity,
          storageRemaining: datanode.storageReport.remaining,
          pipelines: datanode.pipelines,
          containers: datanode.containers,
          leaderCount: datanode.leaderCount,
          version: datanode.version,
          setupTime: datanode.setupTime,
          revision: datanode.revision,
          buildDate: datanode.buildDate
        };
      });

      this.setState({
        loading: false,
        dataSource,
        totalCount,
        lastUpdated: Number(moment())
      });
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });
  };

  componentDidMount(): void {
    // Fetch datanodes on component mount
    this._loadData();
    this.autoReload.startPolling();
  }

  componentWillUnmount(): void {
    this.autoReload.stopPolling();
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
  };

  render() {
    const {dataSource, loading, totalCount, lastUpdated, selectedColumns, columnOptions} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} datanodes`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
      <div className='datanodes-container'>
        <div className='page-header'>
          Datanodes ({totalCount})
          <div className='filter-block'>
            <MultiSelect
              allowSelectAll
              isMulti
              maxShowValues={3}
              className='multi-select-container'
              options={columnOptions}
              closeMenuOnSelect={false}
              hideSelectedOptions={false}
              value={selectedColumns}
              allOption={allColumnsOption}
              onChange={this._handleColumnChange}
            /> Columns
          </div>
          <AutoReloadPanel
            isLoading={loading}
            lastUpdated={lastUpdated}
            togglePolling={this.autoReload.handleAutoReloadToggle}
            onReload={this._loadData}
          />
        </div>

        <div className='content-div'>
          <Table
            dataSource={dataSource}
            columns={COLUMNS.reduce<any[]>((filtered, column) => {
              if (selectedColumns.some(e => e.value === column.key)) {
                if (column.isSearchable) {
                  const newColumn = {
                    ...column,
                    ...new ColumnSearch(column).getColumnSearchProps(column.dataIndex)
                  };
                  filtered.push(newColumn);
                } else {
                  filtered.push(column);
                }
              }

              return filtered;
            }, [])}
            loading={loading}
            pagination={paginationConfig}
            rowKey='hostname'
            scroll={{x: true, y: false, scrollToFirstRowOnChange: true}}
          />
        </div>
      </div>
    );
  }
}
