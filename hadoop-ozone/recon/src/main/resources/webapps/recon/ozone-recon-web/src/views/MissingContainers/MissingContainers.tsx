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
import {Icon, Table, Tooltip} from 'antd';
import './MissingContainers.less';
import {PaginationConfig} from "antd/lib/pagination";
import prettyBytes from "pretty-bytes";
import moment from "moment";
import {timeFormat} from "../../utils/common";

interface MissingContainerResponse {
  containerID: number;
  keys: number;
  replicas: ContainerReplica[];
  missingSince: number;
  pipelineID: string;
}

export interface ContainerReplica {
  containerId: number;
  datanodeHost: string;
  firstReportTimestamp: number;
  lastReportTimestamp: number;
}

export interface MissingContainersResponse  {
  totalCount: number;
  containers: MissingContainerResponse[];
}

interface KeyResponse {
  Volume: string;
  Bucket: string;
  Key: string;
  DataSize: number;
  Versions: number[];
  Blocks: any;
  CreationTime: string;
  ModificationTime: string;
}

interface ContainerKeysResponse {
  totalCount: number;
  keys: KeyResponse[];
}

const COLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: MissingContainerResponse, b: MissingContainerResponse) => a.containerID - b.containerID
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: MissingContainerResponse, b: MissingContainerResponse) => a.keys - b.keys
  },
  {
    title: 'Datanodes',
    dataIndex: 'replicas',
    key: 'replicas',
    render: (replicas: ContainerReplica[]) => <div>
      {replicas.map(replica => {
        const tooltip = <div>
          <div>First Report Time: {timeFormat(replica.firstReportTimestamp)}</div>
          <div>Last Report Time: {timeFormat(replica.lastReportTimestamp)}</div>
        </div>;
        return(
            <div key={replica.datanodeHost}>
              <Tooltip placement="left"
                       title={tooltip}>
                <Icon type="info-circle" className="icon-small"/>
              </Tooltip>
              <span className="pl-5">
                {replica.datanodeHost}
              </span>
            </div>
        );
          }
      )}
    </div>
  },
  {
    title: 'Pipeline ID',
    dataIndex: 'pipelineID',
    key: 'pipelineID',
    sorter: (a: MissingContainerResponse, b: MissingContainerResponse) => a.pipelineID.localeCompare(b.pipelineID)
  },
  {
    title: 'Missing Since',
    dataIndex: 'missingSince',
    key: 'missingSince',
    render: (missingSince: number) => timeFormat(missingSince),
    sorter: (a: MissingContainerResponse, b: MissingContainerResponse) => a.missingSince - b.missingSince
  },
];

const KEY_TABLE_COLUMNS = [
  {
    title: 'Volume',
    dataIndex: 'Volume',
    key: 'Volume'
  },
  {
    title: 'Bucket',
    dataIndex: 'Bucket',
    key: 'Bucket'
  },
  {
    title: 'Key',
    dataIndex: 'Key',
    key: 'Key'
  },
  {
    title: 'Size',
    dataIndex: 'DataSize',
    key: 'DataSize',
    render: (dataSize: number) => <div>{prettyBytes(dataSize)}</div>
  },
  {
    title: 'Date Created',
    dataIndex: 'CreationTime',
    key: 'CreationTime',
    render: (date: string) => moment(date).format('lll')
  },
  {
    title: 'Date Modified',
    dataIndex: 'ModificationTime',
    key: 'ModificationTime',
    render: (date: string) => moment(date).format('lll')
  }
];

interface ExpandedRow {
  [key: number]: ExpandedRowState
}

interface ExpandedRowState {
  containerId: number;
  loading: boolean;
  dataSource: KeyResponse[];
  totalCount: number;
}

interface MissingContainersState {
  loading: boolean;
  dataSource: MissingContainerResponse[];
  totalCount: number;
  expandedRowData: ExpandedRow
}

export class MissingContainers extends React.Component<any, MissingContainersState> {

  constructor(props: any) {
    super(props);
    this.state = {
      loading: false,
      dataSource: [],
      totalCount: 0,
      expandedRowData: {}
    }
  }

  componentDidMount(): void {
    // Fetch missing containers on component mount
    this.setState({
      loading: true
    });
    axios.get('/api/v1/containers/missing').then(response => {
      const missingContainersResponse: MissingContainersResponse = response.data;
      const totalCount = missingContainersResponse.totalCount;
      const missingContainers: MissingContainerResponse[] = missingContainersResponse.containers;
      this.setState({
        loading: false,
        dataSource: missingContainers,
        totalCount: totalCount
      });
    });
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    // TODO: Implement this method once server side pagination is enabled
    console.log(current, pageSize);
  };

  onRowExpandClick = (expanded: boolean, record: MissingContainerResponse) => {
    if (expanded) {
      this.setState(({expandedRowData}) => {
        const expandedRowState: ExpandedRowState = expandedRowData[record.containerID] ?
            Object.assign({}, expandedRowData[record.containerID], {loading: true}) :
            {containerId: record.containerID, loading: true, dataSource: [], totalCount: 0};
        return {
          expandedRowData: Object.assign({}, expandedRowData, {[record.containerID]: expandedRowState})
        }
      });
      axios.get(`/api/v1/containers/${record.containerID}/keys`).then(response => {
        const containerKeysResponse: ContainerKeysResponse = response.data;
        this.setState(({expandedRowData}) => {
          const expandedRowState: ExpandedRowState =
              Object.assign({}, expandedRowData[record.containerID],
                  {loading: false, dataSource: containerKeysResponse.keys, totalCount: containerKeysResponse.totalCount});
          return {
            expandedRowData: Object.assign({}, expandedRowData, {[record.containerID]: expandedRowState})
          }
        });
      });
    }
  };

  expandedRowRender = (record: MissingContainerResponse) => {
    const {expandedRowData} = this.state;
    const containerId = record.containerID;
    if (expandedRowData[containerId]) {
      const containerKeys: ExpandedRowState = expandedRowData[containerId];
      const dataSource = containerKeys.dataSource.map(record => (
          {...record, uid: `${record.Volume}/${record.Bucket}/${record.Key}`}
          ));
      const paginationConfig: PaginationConfig = {
        showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} keys`
      };
      return <Table loading={containerKeys.loading} dataSource={dataSource}
                    columns={KEY_TABLE_COLUMNS} pagination={paginationConfig}
                    rowKey="uid"/>
    }
    return <div>Loading...</div>;
  };

  render () {
    const {dataSource, loading, totalCount} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} missing containers`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
        <div className="missing-containers-container">
          <div className="page-header">
            Missing Containers ({totalCount})
          </div>
          <div className="content-div">
            <Table dataSource={dataSource} columns={COLUMNS} loading={loading} pagination={paginationConfig}
                   rowKey="containerID" expandedRowRender={this.expandedRowRender}
                   expandRowByClick={true} onExpand={this.onRowExpandClick}/>
          </div>
        </div>
    );
  }
}
