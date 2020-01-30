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
import {Table} from 'antd';
import './MissingContainers.less';
import {PaginationConfig} from "antd/lib/pagination";
import prettyBytes from "pretty-bytes";
import moment from "moment";

interface MissingContainerResponse {
  id: number;
  keys: number;
  datanodes: string[];
}

interface MissingContainersResponse  {
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
    dataIndex: 'id',
    key: 'id'
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys'
  },
  {
    title: 'Datanodes',
    dataIndex: 'datanodes',
    key: 'datanodes',
    render: (datanodes: string[]) => <div>{datanodes.map(datanode => <div key={datanode}>{datanode}</div>)}</div>
  }
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
    render: (date: number) => moment(date).format('lll')
  },
  {
    title: 'Date Modified',
    dataIndex: 'ModificationTime',
    key: 'ModificationTime',
    render: (date: number) => moment(date).format('lll')
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
    axios.get('/api/v1/missingContainers').then(response => {
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
        const expandedRowState: ExpandedRowState = expandedRowData[record.id] ?
            Object.assign({}, expandedRowData[record.id], {loading: true}) :
            {containerId: record.id, loading: true, dataSource: [], totalCount: 0};
        return {
          expandedRowData: Object.assign({}, expandedRowData, {[record.id]: expandedRowState})
        }
      });
      axios.get(`/api/v1/containers/${record.id}/keys`).then(response => {
        const containerKeysResponse: ContainerKeysResponse = response.data;
        this.setState(({expandedRowData}) => {
          const expandedRowState: ExpandedRowState =
              Object.assign({}, expandedRowData[record.id],
                  {loading: false, dataSource: containerKeysResponse.keys, totalCount: containerKeysResponse.totalCount});
          return {
            expandedRowData: Object.assign({}, expandedRowData, {[record.id]: expandedRowState})
          }
        });
      });
    }
  };

  expandedRowRender = (record: MissingContainerResponse) => {
    const {expandedRowData} = this.state;
    const containerId = record.id;
    if (expandedRowData[containerId]) {
      const containerKeys: ExpandedRowState = expandedRowData[containerId];
      const paginationConfig: PaginationConfig = {
        showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} keys`
      };
      return <Table loading={containerKeys.loading} dataSource={containerKeys.dataSource}
                    columns={KEY_TABLE_COLUMNS} pagination={paginationConfig}/>
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
                   rowKey="id" expandedRowRender={this.expandedRowRender} expandRowByClick={true} onExpand={this.onRowExpandClick}/>
          </div>
        </div>
    );
  }
}
