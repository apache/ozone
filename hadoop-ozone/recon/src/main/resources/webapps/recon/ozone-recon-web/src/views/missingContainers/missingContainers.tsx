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
import moment from 'moment';
import filesize from 'filesize';
import { Table, Tooltip, Tabs } from 'antd';
import { TablePaginationConfig } from 'antd/es/table';
import { InfoCircleOutlined } from '@ant-design/icons';

import { ColumnSearch } from '@/utils/columnSearch';
import { showDataFetchError, timeFormat } from '@/utils/common';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';

import './missingContainers.less';


const size = filesize.partial({ standard: 'iec' });

interface IContainerResponse {
  containerID: number;
  containerState: string;
  unhealthySince: string;
  expectedReplicaCount: number;
  actualReplicaCount: number;
  replicaDeltaCount: number;
  reason: string;
  keys: number;
  pipelineID: string;
  replicas: IContainerReplicas[];
}

export interface IContainerReplica {
  containerId: number;
  datanodeHost: string;
  firstReportTimestamp: number;
  lastReportTimestamp: number;
}

export interface IContainerReplicas {
  containerId: number;
  datanodeUuid: string;
  datanodeHost: string;
  firstSeenTime: number;
  lastSeenTime: number;
  lastBcsId: number;
}

interface IUnhealthyContainersResponse {
  missingCount: number;
  underReplicatedCount: number;
  overReplicatedCount: number;
  misReplicatedCount: number;
  containers: IContainerResponse[];
}

interface IKeyResponse {
  Volume: string;
  Bucket: string;
  Key: string;
  DataSize: number;
  Versions: number[];
  Blocks: object;
  CreationTime: string;
  ModificationTime: string;
}

interface IContainerKeysResponse {
  totalCount: number;
  keys: IKeyResponse[];
}

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
    render: (dataSize: number) => <div>{size(dataSize)}</div>
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

const CONTAINER_TAB_COLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    isSearchable: true,
    sorter: (a: IContainerResponse, b: IContainerResponse) => a.containerID - b.containerID
  },
  {
    title: 'No. of Blocks',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: IContainerResponse, b: IContainerResponse) => a.keys - b.keys
  },
  {
    title: 'Actual/Expected Replica(s)',
    dataIndex: 'expectedReplicaCount',
    key: 'expectedReplicaCount',
    render: (expectedReplicaCount: number, record: IContainerResponse) => {
      const actualReplicaCount = record.actualReplicaCount;
      return (
        <span>
          {actualReplicaCount} / {expectedReplicaCount}
        </span>
      );
    }
  },
  {
    title: 'Datanodes',
    dataIndex: 'replicas',
    key: 'replicas',
    render: (replicas: IContainerReplicas[]) => (
      <div>
        {replicas && replicas.map(replica => {
          const tooltip = (
            <div>
              <div>First Report Time: {timeFormat(replica.firstSeenTime)}</div>
              <div>Last Report Time: {timeFormat(replica.lastSeenTime)}</div>
            </div>
          );
          return (
            <div key={replica.datanodeHost}>
              <Tooltip
                placement='left'
                title={tooltip}
              >
                <InfoCircleOutlined className='icon-small' />
              </Tooltip>
              <span className='pl-5'>
                {replica.datanodeHost}
              </span>
            </div>
          );
        }
        )}
      </div>
    )
  },
  {
    title: 'Pipeline ID',
    dataIndex: 'pipelineID',
    key: 'pipelineID',
    sorter: (a: IContainerResponse, b: IContainerResponse) => a.pipelineID.localeCompare(b.pipelineID)
  },
  {
    title: 'Unhealthy Since',
    dataIndex: 'unhealthySince',
    key: 'unhealthySince',
    render: (unhealthySince: number) => timeFormat(unhealthySince),
    sorter: (a: IContainerResponse, b: IContainerResponse) => a.unhealthySince - b.unhealthySince
  }
];

interface IExpandedRow {
  [key: number]: IExpandedRowState;
}

interface IExpandedRowState {
  containerId: number;
  loading: boolean;
  dataSource: IKeyResponse[];
  totalCount: number;
}

interface IMissingContainersState {
  loading: boolean;
  missingDataSource: IContainerResponse[];
  underReplicatedDataSource: IContainerResponse[];
  overReplicatedDataSource: IContainerResponse[];
  misReplicatedDataSource: IContainerResponse[];
  expandedRowData: IExpandedRow;
}

let cancelContainerSignal: AbortController;
let cancelRowExpandSignal: AbortController;

export class MissingContainers extends React.Component<Record<string, object>, IMissingContainersState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      loading: false,
      missingDataSource: [],
      underReplicatedDataSource: [],
      overReplicatedDataSource: [],
      misReplicatedDataSource: [],
      expandedRowData: {}
    };
  }

  componentDidMount(): void {
    // Fetch missing containers on component mount
    this.setState({
      loading: true
    });

    const { request, controller } = AxiosGetHelper('/api/v1/containers/unhealthy', cancelContainerSignal);
    cancelContainerSignal = controller;

    request.then(allContainersResponse => {

      const allContainersResponseData: IUnhealthyContainersResponse = allContainersResponse.data;
      const allContainers: IContainerResponse[] = allContainersResponseData.containers;

      const missingContainersResponseData = allContainers && allContainers.filter(item => item.containerState === 'MISSING');
      const mContainers: IContainerResponse[] = missingContainersResponseData;

      const underReplicatedResponseData = allContainers && allContainers.filter(item => item.containerState === 'UNDER_REPLICATED');
      const uContainers: IContainerResponse[] = underReplicatedResponseData;

      const overReplicatedResponseData = allContainers && allContainers.filter(item => item.containerState === 'OVER_REPLICATED');
      const oContainers: IContainerResponse[] = overReplicatedResponseData;

      const misReplicatedResponseData = allContainers && allContainers.filter(item => item.containerState === 'MIS_REPLICATED');
      const mrContainers: IContainerResponse[] = misReplicatedResponseData;

      this.setState({
        loading: false,
        missingDataSource: mContainers,
        underReplicatedDataSource: uContainers,
        overReplicatedDataSource: oContainers,
        misReplicatedDataSource: mrContainers
      });
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });
  }

  componentWillUnmount(): void {
    cancelRequests([
      cancelContainerSignal,
      cancelRowExpandSignal
    ]);
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
  };

  onRowExpandClick = (expanded: boolean, record: IContainerResponse) => {
    if (expanded) {
      this.setState(({ expandedRowData }) => {
        const expandedRowState: IExpandedRowState = expandedRowData[record.containerID]
          ? Object.assign({}, expandedRowData[record.containerID], { loading: true })
          : {
            containerId: record.containerID,
            loading: true,
            dataSource: [],
            totalCount: 0
          };
        return {
          expandedRowData: Object.assign({}, expandedRowData, { [record.containerID]: expandedRowState })
        };
      });

      const { request, controller } = AxiosGetHelper(`/api/v1/containers/${record.containerID}/keys`, cancelRowExpandSignal);
      cancelRowExpandSignal = controller;

      request.then(response => {
        const containerKeysResponse: IContainerKeysResponse = response.data;
        this.setState(({ expandedRowData }) => {
          const expandedRowState: IExpandedRowState =
            Object.assign({}, expandedRowData[record.containerID],
              { loading: false, dataSource: containerKeysResponse.keys, totalCount: containerKeysResponse.totalCount });
          return {
            expandedRowData: Object.assign({}, expandedRowData, { [record.containerID]: expandedRowState })
          };
        });
      }).catch(error => {
        this.setState(({ expandedRowData }) => {
          const expandedRowState: IExpandedRowState =
            Object.assign({}, expandedRowData[record.containerID],
              { loading: false });
          return {
            expandedRowData: Object.assign({}, expandedRowData, { [record.containerID]: expandedRowState })
          };
        });
        showDataFetchError(error.toString());
      });
    }
    else {
      cancelRowExpandSignal && cancelRowExpandSignal.abort();
    }
  };

  expandedRowRender = (record: IContainerResponse) => {
    const { expandedRowData } = this.state;
    const containerId = record.containerID;
    if (expandedRowData[containerId]) {
      const containerKeys: IExpandedRowState = expandedRowData[containerId];
      const dataSource = containerKeys.dataSource.map(record => (
        { ...record, uid: `${record.Volume}/${record.Bucket}/${record.Key}` }
      ));
      const paginationConfig: TablePaginationConfig = {
        showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} keys`
      };
      return (
        <Table
          loading={containerKeys.loading} dataSource={dataSource}
          columns={KEY_TABLE_COLUMNS} pagination={paginationConfig}
          rowKey='uid'
          locale={{ filterTitle: '' }} />
      );
    }

    return <div>Loading...</div>;
  };

  searchColumn = () => {
    return CONTAINER_TAB_COLUMNS.reduce<any[]>((filtered, column) => {
      if (column.isSearchable) {
        const newColumn = {
          ...column,
          ...new ColumnSearch(column).getColumnSearchProps(column.dataIndex)
        };
        filtered.push(newColumn);
      } else {
        filtered.push(column);
      }

      return filtered;
    }, [])
  };

  render() {
    const { missingDataSource, loading, underReplicatedDataSource, overReplicatedDataSource, misReplicatedDataSource } = this.state;
    const paginationConfig: TablePaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} missing containers`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };

    const generateTable = (dataSource) => {
      return <Table
        expandable={{
          expandRowByClick: true,
          expandedRowRender: this.expandedRowRender,
          onExpand: this.onRowExpandClick
        }}
        dataSource={dataSource}
        columns={this.searchColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='containerID'
        locale={{ filterTitle: "" }} />
    }

    return (
      <div className='missing-containers-container'>
        <div className='page-header'>
          Containers
        </div>
        <div className='content-div'>
          <Tabs defaultActiveKey='1'>
            <Tabs.TabPane
              key='1'
              tab={`Missing (${missingDataSource?.length ?? 0})`}>
              {generateTable(missingDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane
              key='2'
              tab={`Under-Replicated (${underReplicatedDataSource?.length ?? 0})`}>
              {generateTable(underReplicatedDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane
              key='3'
              tab={`Over-Replicated (${overReplicatedDataSource?.length ?? 0})`}>
              {generateTable(overReplicatedDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane
              key='4'
              tab={`Mis-Replicated (${misReplicatedDataSource?.length ?? 0})`}>
              {generateTable(misReplicatedDataSource)}
            </Tabs.TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}
