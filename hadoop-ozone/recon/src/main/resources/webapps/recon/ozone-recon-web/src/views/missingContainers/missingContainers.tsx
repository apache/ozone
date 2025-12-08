/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import { Link } from 'react-router-dom';

import {ColumnSearch} from '@/utils/columnSearch';
import {showDataFetchError, timeFormat} from '@/utils/common';
import {AxiosGetHelper, cancelRequests} from '@/utils/axiosRequestHelper';

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
  currentState: string;
  pageSize: number;
  firstSeenKey: number;
  lastSeenKey: number;
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
      currentState: "MISSING",
      pageSize: 10,
      firstSeenKey: 0,
      lastSeenKey: 0
    };
  }

  // --- MODIFIED FUNCTION ---
  // This function now accepts its parameters instead of reading from `this.state`
  fetchUnhealthyContainers = (direction: boolean, options: { pageSize: number, lastSeenKey: number, firstSeenKey: number, currentState: string }) => {
    this.setState({ loading: true });

    const { pageSize, lastSeenKey, firstSeenKey, currentState } = options;

    let baseUrl = `/api/v1/containers/unhealthy/${encodeURIComponent(currentState)}?limit=${pageSize}`;
    let queryParam = direction
        ? `&minContainerId=${encodeURIComponent(lastSeenKey)}`
        : `&maxContainerId=${encodeURIComponent(firstSeenKey)}`;
    let urlVal = baseUrl + queryParam;

    const { request, controller } = AxiosGetHelper(urlVal, cancelContainerSignal);
    cancelContainerSignal = controller;

    request.then(allContainersResponse => {
      const responseData: IUnhealthyContainersResponse = allContainersResponse.data;
      const newContainers = responseData.containers;

      // Use functional setState to avoid race conditions and correctly handle empty responses
      this.setState(prevState => ({
        loading: false,
        containerDataSource: newContainers,
        missingCount: responseData.missingCount,
        misReplicatedCount: responseData.misReplicatedCount,
        underReplicatedCount: responseData.underReplicatedCount,
        overReplicatedCount: responseData.overReplicatedCount,
        firstSeenKey: newContainers.length > 0 ? Number(responseData.firstKey) : prevState.firstSeenKey,
        lastSeenKey: newContainers.length > 0 ? Number(responseData.lastKey) : prevState.lastSeenKey
      }));
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error);
    });
  }

  componentDidMount(): void {
    this.changeTab('1');
  }

  componentWillUnmount(): void {
    cancelRequests([
      cancelContainerSignal,
      cancelRowExpandSignal
    ]);
  }

  onRowExpandClick = (expanded: boolean, record: IContainerResponse) => {
    if (expanded) {
      this.setState(({ expandedRowData }) => {
        const expandedRowState: IExpandedRowState = expandedRowData[record.containerID]
            ? { ...expandedRowData[record.containerID], loading: true }
            : {
              containerId: record.containerID,
              loading: true,
              dataSource: [],
              totalCount: 0
            };
        return {
          expandedRowData: { ...expandedRowData, [record.containerID]: expandedRowState }
        };
      });

      const { request, controller } = AxiosGetHelper(`/api/v1/containers/${record.containerID}/keys`, cancelRowExpandSignal);
      cancelRowExpandSignal = controller;

      request.then(response => {
        const containerKeysResponse: IContainerKeysResponse = response.data;
        this.setState(({ expandedRowData }) => {
          const expandedRowState: IExpandedRowState = {
            ...expandedRowData[record.containerID],
            loading: false,
            dataSource: containerKeysResponse.keys,
            totalCount: containerKeysResponse.totalCount
          };
          return {
            expandedRowData: { ...expandedRowData, [record.containerID]: expandedRowState }
          };
        });
      }).catch(error => {
        this.setState(({ expandedRowData }) => {
          const expandedRowState: IExpandedRowState = {
            ...expandedRowData[record.containerID],
            loading: false
          };
          return {
            expandedRowData: { ...expandedRowData, [record.containerID]: expandedRowState }
          };
        });
        showDataFetchError(error);
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

  onShowSizeChange = (_: number, pageSize: number) => {
    this.setState((prevState) => ({
      pageSize: pageSize,
      lastSeenKey: prevState.firstSeenKey - 1 // We want to stay on the current page range and only change size
    }), () => {
      this.fetchNextRecords();
    });
  }

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
    const { pageSize, lastSeenKey, firstSeenKey, currentState } = this.state;
    this.fetchUnhealthyContainers(false, { pageSize, lastSeenKey, firstSeenKey, currentState });
  }

  fetchNextRecords = () => {
    const { pageSize, lastSeenKey, firstSeenKey, currentState } = this.state;
    this.fetchUnhealthyContainers(true, { pageSize, lastSeenKey, firstSeenKey, currentState });
  }

  itemRender = (_: any, type: string, originalElement: any) => {
    if (type === 'prev') {
      return <div><Link to="#" onClick={this.fetchPreviousRecords}>{'<'}</Link></div>;
    }
    if (type === 'next') {
      return <div><Link to="#" onClick={this.fetchNextRecords}> {'>'} </Link></div>;
    }
    return originalElement;
  };


  changeTab = (activeKey: any) => {
    let currentState = "MISSING";
    if (activeKey === '2') {
      currentState = "UNDER_REPLICATED";
    } else if (activeKey === '3') {
      currentState = "OVER_REPLICATED";
    } else if (activeKey === '4') {
      currentState = "MIS_REPLICATED";
    }
    this.setState({
      containerDataSource: [],
      expandedRowData: {},
      missingCount: 0,
      underReplicatedCount: 0,
      overReplicatedCount: 0,
      misReplicatedCount: 0,
      currentState: currentState,
      firstSeenKey: 0,
      lastSeenKey: 0
    }, () => {
      this.fetchNextRecords();
    });
  }


  render() {
    const {containerDataSource, loading,
      missingCount, misReplicatedCount, overReplicatedCount, underReplicatedCount} = this.state;

    // --- MODIFIED PAGINATION CONFIG ---
    const paginationConfig: TablePaginationConfig = {
      pageSize: this.state.pageSize,
      pageSizeOptions: ['10', '20', '30', '50'],
      showSizeChanger: true,
      itemRender: this.itemRender,
      onShowSizeChange: this.onShowSizeChange,
      // Removed current and onChange as they are not needed for cursor-based pagination
    };

    const generateTable = (dataSource: IContainerResponse[]) => {
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
