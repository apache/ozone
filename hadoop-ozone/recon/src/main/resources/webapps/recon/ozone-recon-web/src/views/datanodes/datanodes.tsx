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
import {DatanodeStatus, IStorageReport} from 'types/datanode.types';
import './datanodes.less';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';
import {showDataFetchError} from 'utils/common';

interface IDatanodeResponse {
  hostname: string;
  state: DatanodeStatus;
  lastHeartbeat: number;
  storageReport: IStorageReport;
  pipelines: IPipeline[];
  containers: number;
  leaderCount: number;
}

interface IDatanodesResponse {
  totalCount: number;
  datanodes: IDatanodeResponse[];
}

interface IDatanode {
  hostname: string;
  state: DatanodeStatus;
  lastHeartbeat: number;
  storageUsed: number;
  storageTotal: number;
  storageRemaining: number;
  pipelines: IPipeline[];
  containers: number;
  leaderCount: number;
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
}

const renderDatanodeStatus = (status: DatanodeStatus) => {
  const statusIconMap = {
    HEALTHY: <Icon type='check-circle' theme='filled' twoToneColor='#1da57a' className='icon-success'/>,
    STALE: <Icon type='hourglass' theme='filled' className='icon-warning'/>,
    DEAD: <Icon type='close-circle' theme='filled' className='icon-failure'/>,
    DECOMMISSIONING: <Icon type='warning' theme='filled' className='icon-warning'/>,
    DECOMMISSIONED: <Icon type='exclamation-circle' theme='filled' className='icon-failure'/>
  };
  const icon = status in statusIconMap ? statusIconMap[status] : '';
  return <span>{icon} {status}</span>;
};

const COLUMNS = [
  {
    title: 'Status',
    dataIndex: 'state',
    key: 'state',
    render: (text: DatanodeStatus) => renderDatanodeStatus(text),
    sorter: (a: IDatanode, b: IDatanode) => a.state.localeCompare(b.state)
  },
  {
    title: 'Hostname',
    dataIndex: 'hostname',
    key: 'hostname',
    sorter: (a: IDatanode, b: IDatanode) => a.hostname.localeCompare(b.hostname),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Storage Capacity',
    dataIndex: 'storageUsed',
    key: 'storageUsed',
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
    sorter: (a: IDatanode, b: IDatanode) => a.lastHeartbeat - b.lastHeartbeat,
    render: (heartbeat: number) => {
      return heartbeat > 0 ? moment(heartbeat).format('lll') : 'NA';
    }
  },
  {
    title: 'Pipeline ID(s)',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: IPipeline[], record: IDatanode) => {
      return (
        <div>
          {
            pipelines.map((pipeline, index) => (
              <div key={index} className='pipeline-container'>
                <ReplicationIcon replicationFactor={pipeline.replicationFactor}
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
    title: <span>
      Leader Count&nbsp;
      <Tooltip title='The number of Ratis Pipelines in which the given datanode is elected as a leader.'>
        <Icon type='info-circle'/>
      </Tooltip>
    </span>,
    dataIndex: 'leaderCount',
    key: 'leaderCount',
    sorter: (a: IDatanode, b: IDatanode) => a.leaderCount - b.leaderCount
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers',
    sorter: (a: IDatanode, b: IDatanode) => a.containers - b.containers
  }
];

export class Datanodes extends React.Component<Record<string, object>, IDatanodesState> {
  autoReload: AutoReloadHelper;

  constructor(props = {}) {
    super(props);
    this.state = {
      loading: false,
      dataSource: [],
      totalCount: 0,
      lastUpdated: 0
    };
    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _loadData = () => {
    this.setState({
      loading: true
    });
    axios.get('/api/v1/datanodes').then(response => {
      const datanodesResponse: IDatanodesResponse = response.data;
      const totalCount = datanodesResponse.totalCount;
      const datanodes: IDatanodeResponse[] = datanodesResponse.datanodes;
      const dataSource: IDatanode[] = datanodes.map(datanode => {
        return {
          hostname: datanode.hostname,
          state: datanode.state,
          lastHeartbeat: datanode.lastHeartbeat,
          storageUsed: datanode.storageReport.used,
          storageTotal: datanode.storageReport.capacity,
          storageRemaining: datanode.storageReport.remaining,
          pipelines: datanode.pipelines,
          containers: datanode.containers,
          leaderCount: datanode.leaderCount
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
    const {dataSource, loading, totalCount, lastUpdated} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} datanodes`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
      <div className='datanodes-container'>
        <div className='page-header'>
          Datanodes ({totalCount})
          <AutoReloadPanel isLoading={loading} lastUpdated={lastUpdated} togglePolling={this.autoReload.handleAutoReloadToggle} onReload={this._loadData}/>
        </div>
        <div className='content-div'>
          <Table dataSource={dataSource} columns={COLUMNS} loading={loading} pagination={paginationConfig} rowKey='hostname'/>
        </div>
      </div>
    );
  }
}
