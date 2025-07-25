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
import {Table, Tabs, Tooltip} from 'antd';
import {TablePaginationConfig} from 'antd/es/table';
import {InfoCircleOutlined} from '@ant-design/icons';

import {ColumnSearch} from '@/utils/columnSearch';
import {showDataFetchError, timeFormat} from '@/utils/common';
import {AxiosGetHelper, cancelRequests} from '@/utils/axiosRequestHelper';

import './missingContainers.less';


const size = filesize.partial({ standard: 'iec' });
const total_fetch_entries = 1000;

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
  lastKey: string;
  firstKey: string;
}

interface IKeyResponse {
  Volume: string;
  Bucket: string;
  Key: string;
  CompletePath: string;
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
    title: 'Path',
    dataIndex: 'CompletePath',
    key: 'CompletePath',
    width: '270px'
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
  containerDataSource: IContainerResponse[];
  missingCount: number;
  underReplicatedCount: number;
  overReplicatedCount: number;
  misReplicatedCount: number;
  expandedRowData: IExpandedRow;
  DEFAULT_LIMIT: number;
  currentState: string;
  firstSeenKey: string;
  lastSeenKey: string;
  currentPage: number;
}

let cancelContainerSignal: AbortController;
let cancelRowExpandSignal: AbortController;

export class MissingContainers extends React.Component<Record<string, object>, IMissingContainersState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      loading: false,
      containerDataSource: [],
      expandedRowData: {},
      missingCount: 0,
      underReplicatedCount: 0,
      overReplicatedCount: 0,
      misReplicatedCount: 0,
      DEFAULT_LIMIT: 10,
      currentState: "MISSING",
      firstSeenKey: '0',
      lastSeenKey: '0',
      currentPage: 1
    };
  }

  fetchUnhealthyContainers = (direction: boolean) => {
    this.setState({
      currentPage: 1,
      loading: true
    });
    let baseUrl =
        `/api/v1/containers/unhealthy/${encodeURIComponent(this.state.currentState)}?limit=${total_fetch_entries}`;
    let queryParam = direction
        ? `&minContainerId=${encodeURIComponent(this.state.lastSeenKey)}`
        : `&maxContainerId=${encodeURIComponent(this.state.firstSeenKey)}`;
    let urlVal = baseUrl + queryParam;
    const { request, controller } = AxiosGetHelper(urlVal, cancelContainerSignal);
    cancelContainerSignal = controller;

    request.then(allContainersResponse => {

      const allContainersResponseData: IUnhealthyContainersResponse = allContainersResponse.data;
      let allContainers: IContainerResponse[] = allContainersResponseData.containers;
      let firstSeenKey = this.state.firstSeenKey;
      let lastSeenKey = this.state.lastSeenKey
      if (allContainers.length > 0) {
        firstSeenKey = allContainersResponseData.firstKey;
        lastSeenKey = allContainersResponseData.lastKey;
      } else {
       allContainers = this.state.containerDataSource
      }

      this.setState({
        loading: false,
        containerDataSource: allContainers,
        missingCount: allContainersResponseData.missingCount,
        misReplicatedCount: allContainersResponseData.misReplicatedCount,
        underReplicatedCount: allContainersResponseData.underReplicatedCount,
        overReplicatedCount: allContainersResponseData.overReplicatedCount,
        firstSeenKey: firstSeenKey,
        lastSeenKey: lastSeenKey,
        currentPage: 1
      });
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });


  }

  componentDidMount(): void {
    this.changeTab('1')
  }

  componentWillUnmount(): void {
    cancelRequests([
      cancelContainerSignal,
      cancelRowExpandSignal
    ]);
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
    this.setState({
      currentPage: 1,
      DEFAULT_LIMIT : pageSize,
    })
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


  fetchPreviousRecords = () => {
    this.fetchUnhealthyContainers(false)
  }

  fetchNextRecords = () => {
    this.fetchUnhealthyContainers(true)
  }

  itemRender = (_: any, type: string, originalElement: any) => {
    if (type === 'prev') {
      return <div><Link to="/Containers" onClick={this.fetchPreviousRecords}>{'<'}</Link></div>;
    }
    if (type === 'next') {
      return <div><Link to="/Containers" onClick={this.fetchNextRecords}> {'>'} </Link></div>;
    }
    return originalElement;
  };


  changeTab = (activeKey: any) => {
    let currentState = "MISSING"
    if (activeKey == '2') {
      currentState = "UNDER_REPLICATED"
    } else if (activeKey == '3') {
      currentState = "OVER_REPLICATED"
    } else if (activeKey == '4') {
      currentState = "MIS_REPLICATED"
    }
    this.setState({
      loading: false,
      containerDataSource: [],
      expandedRowData: {},
      missingCount: 0,
      underReplicatedCount: 0,
      overReplicatedCount: 0,
      misReplicatedCount: 0,
      DEFAULT_LIMIT: 10,
      currentState: currentState,
      firstSeenKey: 0,
      lastSeenKey: 0,
    }, () => {
      this.fetchNextRecords();
    })

  }


  render() {
    const {containerDataSource, loading,
    missingCount, misReplicatedCount, overReplicatedCount, underReplicatedCount} = this.state;
    const paginationConfig: TablePaginationConfig = {
      current: this.state.currentPage,
      pageSize:this.state.DEFAULT_LIMIT,
      defaultPageSize: this.state.DEFAULT_LIMIT,
      pageSizeOptions: ['10', '20', '30', '50'],
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange,
      itemRender: this.itemRender,
      onChange: (page: number) => this.setState({ currentPage: page })
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
          <Tabs defaultActiveKey='1' onChange={this.changeTab}>
            <Tabs.TabPane
                key='1'
                tab={`Missing${(missingCount > 0) ? ` (${missingCount})` : ''}`}>
                {generateTable(containerDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane
                key='2'
                tab={`Under-Replicated${(underReplicatedCount > 0) ? ` (${underReplicatedCount})` : ''}`}>
                {generateTable(containerDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane
                key='3'
                tab={`Over-Replicated${(overReplicatedCount > 0) ? ` (${overReplicatedCount})` : ''}`}>
                {generateTable(containerDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane
                key='4'
                tab={`Mis-Replicated${(misReplicatedCount > 0) ? ` (${misReplicatedCount})` : ''}`}>
                {generateTable(containerDataSource)}
            </Tabs.TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}
