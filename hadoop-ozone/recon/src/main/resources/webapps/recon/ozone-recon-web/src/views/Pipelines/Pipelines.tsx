/**
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
import './Pipelines.less';
import {PaginationConfig} from "antd/lib/pagination";
import prettyMilliseconds from "pretty-ms";
import moment from 'moment';
const {TabPane} = Tabs;

export type PipelineStatus = "active" | "inactive";

interface PipelineResponse {
  pipelineId: string;
  status: PipelineStatus;
  leaderNode: string;
  datanodes: string[];
  lastLeaderElection: number;
  duration: number;
  leaderElections: number;
}

interface PipelinesResponse  {
  totalCount: number;
  pipelines: PipelineResponse[];
}

interface PipelinesState {
  activeLoading: boolean;
  activeDataSource: PipelineResponse[];
  activeTotalCount: number;
  inactiveLoading: boolean;
  inactiveDataSource: PipelineResponse[];
  inactiveTotalCount: number;
}

const COLUMNS = [
  {
    title: 'Pipeline ID',
    dataIndex: 'pipelineId',
    key: 'pipelineId'
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
    key: 'leaderNode'
  },
  {
    title: 'Last Leader Election',
    dataIndex: 'lastLeaderElection',
    key: 'lastLeaderElection',
    render: (lastLeaderElection: number) => moment(lastLeaderElection).format('lll')
  },
  {
    title: 'Lifetime',
    dataIndex: 'duration',
    key: 'duration',
    render: (duration: number) => prettyMilliseconds(duration, {compact: true})
  },
  {
    title: 'No. of Elections',
    dataIndex: 'leaderElections',
    key: 'leaderElections'
  }
];

export class Pipelines extends React.Component<any, PipelinesState> {

  constructor(props: any) {
    super(props);
    this.state = {
      activeLoading: false,
      activeDataSource: [],
      activeTotalCount: 0,
      inactiveLoading: false,
      inactiveDataSource: [],
      inactiveTotalCount: 0
    }
  }

  componentDidMount(): void {
    // Fetch pipelines on component mount
    this.setState({
      activeLoading: true
    });
    axios.get('/api/v1/pipelines').then(response => {
      const pipelinesResponse: PipelinesResponse = response.data;
      const totalCount = pipelinesResponse.totalCount;
      const pipelines: PipelineResponse[] = pipelinesResponse.pipelines;
      this.setState({
        activeLoading: false,
        activeDataSource: pipelines,
        activeTotalCount: totalCount
      });
    });
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    // TODO: Implement this method once server side pagination is enabled
    console.log(current, pageSize);
  };

  onTabChange = (activeKey: string) => {
    // Fetch inactive pipelines if tab is switched to "Inactive"
    if (activeKey === "2") {
      // TODO: Trigger an ajax request to fetch inactive pipelines
    }
  };

  render () {
    const {activeDataSource, activeLoading, activeTotalCount} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} pipelines`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
        <div className="pipelines-container">
          <div className="page-header">
            Pipelines ({activeTotalCount})
          </div>
          <div className="content-div">
            <Tabs defaultActiveKey="1" onChange={this.onTabChange}>
              <TabPane key="1" tab="Active">
                <Table dataSource={activeDataSource} columns={COLUMNS} loading={activeLoading} pagination={paginationConfig} rowKey="pipelineId"/>
              </TabPane>
              <TabPane key="2" tab="Inactive">

              </TabPane>
            </Tabs>
          </div>
        </div>
    );
  }
}
