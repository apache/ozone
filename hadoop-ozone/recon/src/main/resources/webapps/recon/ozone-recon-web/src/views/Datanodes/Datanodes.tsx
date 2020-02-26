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
import filesize from 'filesize';
import './Datanodes.less';
import {PaginationConfig} from 'antd/lib/pagination';
import moment from 'moment';
import {getCapacityPercent} from 'utils/common';
import Tooltip from 'antd/lib/tooltip';
import {FilledIcon} from 'utils/themeIcons';

export type DatanodeStatus = "HEALTHY" | "STALE" | "DEAD" | "DECOMMISSIONING" | "DECOMMISSIONED";
const size = filesize.partial({standard: 'iec', round: 1});

interface StorageReport {
  capacity: number;
  used: number;
  remaining: number;
}

interface DatanodeResponse {
  hostname: string;
  state: DatanodeStatus;
  lastHeartbeat: number;
  storageReport: StorageReport;
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
  storageRemaining: number;
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
    render: (text: DatanodeStatus) => renderDatanodeStatus(text),
    sorter: (a: Datanode, b: Datanode) => a.state.localeCompare(b.state)
  },
  {
    title: 'Hostname',
    dataIndex: 'hostname',
    key: 'hostname',
    sorter: (a: Datanode, b: Datanode) => a.hostname.localeCompare(b.hostname),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Storage Capacity',
    dataIndex: 'storageUsed',
    key: 'storageUsed',
    sorter: (a: Datanode, b: Datanode) => a.storageRemaining - b.storageRemaining,
    render: (text: string, record: Datanode) => {
      const nonOzoneUsed = record.storageTotal - record.storageRemaining - record.storageUsed;
      const totalUsed = record.storageTotal - record.storageRemaining;
      const tooltip = <div>
        <p><Icon component={FilledIcon} className="ozone-used-bg"/> Ozone Used ({size(record.storageUsed)})</p>
        <p><Icon component={FilledIcon} className="non-ozone-used-bg"/> Non Ozone Used ({size(nonOzoneUsed)})</p>
        <p><Icon component={FilledIcon} className="remaining-bg"/> Remaining ({size(record.storageRemaining)})</p>
      </div>;
      return <div className="storage-cell-container">
        <Tooltip title={tooltip} placement="bottomLeft">
          <div>{size(record.storageUsed)} + {size(nonOzoneUsed)} / {size(record.storageTotal)}</div>
          <Progress strokeLinecap="square"
                    percent={getCapacityPercent(totalUsed, record.storageTotal)}
                    successPercent={getCapacityPercent(record.storageUsed, record.storageTotal)}
                    className="capacity-bar" strokeWidth={3}/>
        </Tooltip>
      </div>
    }
  },
  {
    title: 'Last Heartbeat',
    dataIndex: 'lastHeartbeat',
    key: 'lastHeartbeat',
    sorter: (a: Datanode, b: Datanode) => a.lastHeartbeat - b.lastHeartbeat,
    render: (heartbeat: number) => {
      return heartbeat > 0 ? moment(heartbeat).format('lll') : 'NA';
    }
  },
  {
    title: 'Pipeline ID(s)',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: string[]) => <div>{pipelines.map((pipeline, index) => <div key={index}>{pipeline}</div>)}</div>
  },
  {
    title: 'Containers',
    dataIndex: 'containers',
    key: 'containers',
    sorter: (a: Datanode, b: Datanode) => a.containers - b.containers
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
        return {
          hostname: datanode.hostname,
          state: datanode.state,
          lastHeartbeat: datanode.lastHeartbeat,
          storageUsed: datanode.storageReport.used,
          storageTotal: datanode.storageReport.capacity,
          storageRemaining: datanode.storageReport.remaining,
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
