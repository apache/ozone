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
import {Table, Tabs} from 'antd';
import './pipelines.less';
import {PaginationConfig} from 'antd/lib/pagination';
import prettyMilliseconds from 'pretty-ms';
import moment from 'moment';
import {ReplicationIcon} from 'utils/themeIcons';
import {AutoReloadHelper} from 'utils/autoReloadHelper';
import AutoReloadPanel from 'components/autoReloadPanel/autoReloadPanel';
import {showDataFetchError} from 'utils/common';
import {IAxiosResponse} from 'types/axios.types';

const {TabPane} = Tabs;
export type PipelineStatus = 'active' | 'inactive';

interface IPipelineResponse {
  pipelineId: string;
  status: PipelineStatus;
  replicationType: string;
  leaderNode: string;
  datanodes: string[];
  lastLeaderElection: number;
  duration: number;
  leaderElections: number;
  replicationFactor: number;
  containers: number;
}

interface IPipelinesResponse {
  totalCount: number;
  pipelines: IPipelineResponse[];
}

interface IPipelinesState {
  activeLoading: boolean;
  activeDataSource: IPipelineResponse[];
  activeTotalCount: number;
  lastUpdated: number;
}

const COLUMNS = [
  {
    title: 'Pipeline ID',
    dataIndex: 'pipelineId',
    key: 'pipelineId',
    sorter: (a: IPipelineResponse, b: IPipelineResponse) => a.pipelineId.localeCompare(b.pipelineId)
  },
  {
    title: 'Replication Type & Factor',
    dataIndex: 'replicationType',
    key: 'replicationType',
    render: (replicationType: string, record: IPipelineResponse) => {
      const replicationFactor = record.replicationFactor;
      return (
        <span>
          <ReplicationIcon replicationFactor={replicationFactor} replicationType={replicationType}/>
          {replicationType} ({replicationFactor})
        </span>
      );
    },
    sorter: (a: IPipelineResponse, b: IPipelineResponse) =>
      (a.replicationType + a.replicationFactor.toString()).localeCompare(b.replicationType + b.replicationFactor.toString()),
    defaultSortOrder: 'descend' as const
  },
  {
    title: 'Status',
    dataIndex: 'status',
    key: 'status',
    sorter: (a: IPipelineResponse, b: IPipelineResponse) => a.status.localeCompare(b.status)
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers',
    sorter: (a: IPipelineResponse, b: IPipelineResponse) => a.containers - b.containers
  },
  {
    title: 'Datanodes',
    dataIndex: 'datanodes',
    key: 'datanodes',
    render: (datanodes: string[]) => <div>{datanodes.map(datanode => <div key={datanode}>{datanode}</div>)}</div>
  },
  {
    title: 'Leader',
    dataIndex: 'leaderNode',
    key: 'leaderNode',
    sorter: (a: IPipelineResponse, b: IPipelineResponse) => a.leaderNode.localeCompare(b.leaderNode)
  },
  {
    title: 'Last Leader Election',
    dataIndex: 'lastLeaderElection',
    key: 'lastLeaderElection',
    render: (lastLeaderElection: number) => lastLeaderElection > 0 ?
      moment(lastLeaderElection).format('lll') : 'NA',
    sorter: (a: IPipelineResponse, b: IPipelineResponse) => a.lastLeaderElection - b.lastLeaderElection
  },
  {
    title: 'Lifetime',
    dataIndex: 'duration',
    key: 'duration',
    render: (duration: number) => prettyMilliseconds(duration, {compact: true}),
    sorter: (a: IPipelineResponse, b: IPipelineResponse) => a.duration - b.duration
  },
  {
    title: 'No. of Elections',
    dataIndex: 'leaderElections',
    key: 'leaderElections',
    sorter: (a: IPipelineResponse, b: IPipelineResponse) => a.leaderElections - b.leaderElections
  }
];

export class Pipelines extends React.Component<Record<string, object>, IPipelinesState> {
  autoReload: AutoReloadHelper;

  constructor(props = {}) {
    super(props);
    this.state = {
      activeLoading: false,
      activeDataSource: [],
      activeTotalCount: 0,
      lastUpdated: 0
    };
    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _loadData = () => {
    this.setState({
      activeLoading: true
    });
    axios.get('/api/v1/pipelines').then((response: IAxiosResponse<IPipelinesResponse>) => {
      const pipelinesResponse: IPipelinesResponse = response.data;
      const totalCount = pipelinesResponse.totalCount;
      const pipelines: IPipelineResponse[] = pipelinesResponse.pipelines;
      this.setState({
        activeLoading: false,
        activeDataSource: pipelines,
        activeTotalCount: totalCount,
        lastUpdated: Number(moment())
      });
    }).catch(error => {
      this.setState({
        activeLoading: false
      });
      showDataFetchError(error.toString());
    });
  };

  componentDidMount(): void {
    // Fetch pipelines on component mount
    this._loadData();
    this.autoReload.startPolling();
  }

  componentWillUnmount(): void {
    this.autoReload.stopPolling();
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
  };

  onTabChange = (activeKey: string) => {
    // Fetch inactive pipelines if tab is switched to "Inactive"
    if (activeKey === '2') {
      // Fetch inactive pipelines in the future
    }
  };

  render() {
    const {activeDataSource, activeLoading, activeTotalCount, lastUpdated} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} pipelines`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
      <div className='pipelines-container'>
        <div className='page-header'>
          Pipelines ({activeTotalCount})
          <AutoReloadPanel isLoading={activeLoading} lastUpdated={lastUpdated} togglePolling={this.autoReload.handleAutoReloadToggle} onReload={this._loadData}/>
        </div>
        <div className='content-div'>
          <Tabs defaultActiveKey='1' onChange={this.onTabChange}>
            <TabPane key='1' tab='Active'>
              <Table dataSource={activeDataSource} columns={COLUMNS} loading={activeLoading} pagination={paginationConfig} rowKey='pipelineId'/>
            </TabPane>
            <TabPane key='2' tab='Inactive'/>
          </Tabs>
        </div>
      </div>
    );
  }
}
