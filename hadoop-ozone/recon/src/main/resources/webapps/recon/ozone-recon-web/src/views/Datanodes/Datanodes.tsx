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
import {Table, Icon, Progress} from 'antd';
import prettyBytes from 'pretty-bytes';
import './Datanodes.less';
import {PaginationConfig} from "antd/lib/pagination";
import moment from 'moment';
import {getCapacityPercent} from "utils/common";

export type DatanodeStatus = "HEALTHY" | "STALE" | "DEAD" | "DECOMMISSIONING" | "DECOMMISSIONED";

interface StorageReport {
  storageLocation: string;
  total: number;
  used: number;
  remaining: number;
}

interface DatanodeResponse {
  hostname: string;
  state: DatanodeStatus;
  lastHeartbeat: number;
  storageReport: StorageReport[];
  pipelineIDs: string[];
  containers: number;
}

interface DatanodesResponse  {
  totalCount: number;
  datanodes: DatanodeResponse[];
}

interface Datanode {
  hostname: string;
  state: DatanodeStatus;
  lastHeartbeat: number;
  storageUsed: number;
  storageTotal: number;
  pipelines: string[];
  containers: number;
}

interface DatanodesState {
  loading: boolean;
  dataSource: Datanode[];
  totalCount: number;
}

const renderDatanodeStatus = (status: DatanodeStatus) => {
  const statusIconMap = {
    HEALTHY: <Icon type="check-circle" theme="filled" twoToneColor="#1da57a" className="icon-success"/>,
    STALE: <Icon type="hourglass" theme="filled" className="icon-warning"/>,
    DEAD: <Icon type="close-circle" theme="filled" className="icon-failure"/>,
    DECOMMISSIONING: <Icon type="warning" theme="filled" className="icon-warning"/>,
    DECOMMISSIONED: <Icon type="exclamation-circle" theme="filled" className="icon-failure"/>
  };
  const icon = status in statusIconMap ? statusIconMap[status] : '';
  return <span>{icon} {status}</span>;
};

const COLUMNS = [
  {
    title: 'Status',
    dataIndex: 'state',
    key: 'state',
    render: (text: DatanodeStatus) => renderDatanodeStatus(text)
  },
  {
    title: 'Hostname',
    dataIndex: 'hostname',
    key: 'hostname'
  },
  {
    title: 'Storage Capacity',
    dataIndex: 'storageUsed',
    key: 'storageUsed',
    render: (text: string, record: Datanode) => <div className="storage-cell-container">
      <div>{prettyBytes(record.storageUsed)} / {prettyBytes(record.storageTotal)}</div>
      <Progress strokeLinecap="square" percent={getCapacityPercent(record.storageUsed, record.storageTotal)}
                className="capacity-bar" strokeWidth={3}/>
    </div>
  },
  {
    title: 'Last Heartbeat',
    dataIndex: 'lastHeartbeat',
    key: 'lastHeartbeat',
    render: (heartbeat: number) => moment(heartbeat).format('lll')
  },
  {
    title: 'Pipeline ID(s)',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: string[]) => <div>{pipelines.map(pipeline => <div>{pipeline}</div>)}</div>
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers'
  }
];

export class Datanodes extends React.Component<any, DatanodesState> {

  constructor(props: any) {
    super(props);
    this.state = {
      loading: false,
      dataSource: [],
      totalCount: 0
    }
  }

  componentDidMount(): void {
    // Fetch datanodes on component mount
    this.setState({
      loading: true
    });
    axios.get('/api/v1/datanodes').then(response => {
      const datanodesResponse: DatanodesResponse = response.data;
      const totalCount = datanodesResponse.totalCount;
      const datanodes: DatanodeResponse[] = datanodesResponse.datanodes;
      const dataSource: Datanode[] = datanodes.map(datanode => {
        const storageTotal = datanode.storageReport.reduce((sum: number, storageReport) => {
          return sum  + storageReport.total;
        }, 0);
        const storageUsed = datanode.storageReport.reduce((sum: number, storageReport) => {
          return sum  + storageReport.used;
        }, 0);
        return {
          hostname: datanode.hostname,
          state: datanode.state,
          lastHeartbeat: datanode.lastHeartbeat,
          storageUsed,
          storageTotal,
          pipelines: datanode.pipelineIDs,
          containers: datanode.containers
        }
      });
      this.setState({
        loading: false,
        dataSource,
        totalCount
      });
    });
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    // TODO: Implement this method once server side pagination is enabled
    console.log(current, pageSize);
  };

  render () {
    const {dataSource, loading, totalCount} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} datanodes`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
        <div className="datanodes-container">
          <div className="page-header">
            Datanodes ({totalCount})
          </div>
          <div className="content-div">
            <Table dataSource={dataSource} columns={COLUMNS} loading={loading} pagination={paginationConfig} rowKey="hostname"/>
          </div>
        </div>
    );
  }
}
