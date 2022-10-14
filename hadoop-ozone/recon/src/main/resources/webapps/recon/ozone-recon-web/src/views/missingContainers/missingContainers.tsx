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
import {Icon, Table, Tooltip, Tabs} from 'antd';
import {PaginationConfig} from 'antd/lib/pagination';
import filesize from 'filesize';
import moment from 'moment';
import {showDataFetchError, timeFormat} from 'utils/common';
import './missingContainers.less';

const size = filesize.partial({standard: 'iec'});
const {TabPane} = Tabs;

interface IMissingContainerResponse {
  containerID: number;
  keys: number;
  replicas: IContainerReplica[];
  missingSince: number;
  pipelineID: string;
}

interface IAllContainerResponse {
  containerID: number;
  containerState: string;
  unhealthySince: string;
  expectedReplicaCount: number;
  actualReplicaCount: number;
  replicaDeltaCount: number;
  reason: string;
  keys: number;
  pipelineID: string;
  replicas: IContainerReplicaAll[];
}

export interface IContainerReplica {
  containerId: number;
  datanodeHost: string;
  firstReportTimestamp: number;
  lastReportTimestamp: number;
}

export interface IContainerReplicaAll {
  containerId: number;
  datanodeUuid: string;
  datanodeHost: string;
  firstSeenTime: number;
  lastSeenTime: number;
  lastBcsId: number;
}

export interface IMissingContainersResponse {
  totalCount: number;
  containers: IMissingContainerResponse[];
}

interface IAllReplicatedContainersResponse {
  missingCount: number;
  underReplicatedCount: number;
  overReplicatedCount: number;
  misReplicatedCount: number;
  containers: IAllContainerResponse[];
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

const COLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: IMissingContainerResponse, b: IMissingContainerResponse) => a.containerID - b.containerID
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: IMissingContainerResponse, b: IMissingContainerResponse) => a.keys - b.keys
  },
  {
    title: 'Datanodes',
    dataIndex: 'replicas',
    key: 'replicas',
    render: (replicas: IContainerReplica[]) => (
      <div>
        {replicas && replicas.map(replica => {
          const tooltip = (
            <div>
              <div>First Report Time: {timeFormat(replica.firstReportTimestamp)}</div>
              <div>Last Report Time: {timeFormat(replica.lastReportTimestamp)}</div>
            </div>
          );
          return (
            <div key={replica.datanodeHost}>
              <Tooltip
                placement='left'
                title={tooltip}
              >
                <Icon type='info-circle' className='icon-small'/>
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
    sorter: (a: IMissingContainerResponse, b: IMissingContainerResponse) => a.pipelineID.localeCompare(b.pipelineID)
  },
  {
    title: 'Missing Since',
    dataIndex: 'missingSince',
    key: 'missingSince',
    render: (missingSince: number) => timeFormat(missingSince),
    sorter: (a: IMissingContainerResponse, b: IMissingContainerResponse) => a.missingSince - b.missingSince
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

const UCOLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.containerID - b.containerID
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.keys - b.keys
  },
  {
    title: 'Active/Expected Replica(s)',
    dataIndex: 'expectedReplicaCount',
    key: 'expectedReplicaCount',
    render: (expectedReplicaCount: number, record: IAllContainerResponse) => {
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
    render: (replicas: IContainerReplicaAll[]) => (
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
                <Icon type='info-circle' className='icon-small'/>
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
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.pipelineID.localeCompare(b.pipelineID)
  },
  {
    title: 'Unhealthy Since',
    dataIndex: 'unhealthySince',
    key: 'unhealthySince',
    render: (unhealthySince: number) => timeFormat(unhealthySince),
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.unhealthySince - b.unhealthySince
  }
];

const OCOLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.containerID - b.containerID
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.keys - b.keys
  },
  {
    title: 'Active/Expected Replica(s)',
    dataIndex: 'expectedReplicaCount',
    key: 'expectedReplicaCount',
    render: (expectedReplicaCount: number, record: IAllContainerResponse) => {
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
    render: (replicas: IContainerReplicaAll[]) => (
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
                <Icon type='info-circle' className='icon-small'/>
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
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.pipelineID.localeCompare(b.pipelineID)
  },
  {
    title: 'Unhealthy Since',
    dataIndex: 'unhealthySince',
    key: 'unhealthySince',
    render: (unhealthySince: number) => timeFormat(unhealthySince),
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.unhealthySince - b.unhealthySince
  }
];

const MCOLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.containerID - b.containerID
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.keys - b.keys
  },
  {
    title: 'Active/Expected Replica(s)',
    dataIndex: 'expectedReplicaCount',
    key: 'expectedReplicaCount',
    render: (expectedReplicaCount: number, record: IAllContainerResponse) => {
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
    render: (replicas: IContainerReplicaAll[]) => (
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
                <Icon type='info-circle' className='icon-small'/>
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
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.pipelineID.localeCompare(b.pipelineID)
  },
  {
    title: 'Unhealthy Since',
    dataIndex: 'unhealthySince',
    key: 'unhealthySince',
    render: (unhealthySince: number) => timeFormat(unhealthySince),
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.unhealthySince - b.unhealthySince
  }
];

const ACOLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.containerID - b.containerID
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.keys - b.keys
  },
  {
    title: 'Active/Expected Replica(s)',
    dataIndex: 'expectedReplicaCount',
    key: 'expectedReplicaCount',
    render: (expectedReplicaCount: number, record: IAllContainerResponse) => {
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
    render: (replicas: IContainerReplicaAll[]) => (
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
                <Icon type='info-circle' className='icon-small'/>
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
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.pipelineID.localeCompare(b.pipelineID)
  },
  {
    title: 'Unhealthy Since',
    dataIndex: 'unhealthySince',
    key: 'unhealthySince',
    render: (unhealthySince: number) => timeFormat(unhealthySince),
    sorter: (a: IAllContainerResponse, b: IAllContainerResponse) => a.unhealthySince - b.unhealthySince
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
  dataSource: IMissingContainerResponse[];
  udataSource: IAllContainerResponse[];
  odataSource: IAllContainerResponse[];
  mdataSource: IAllContainerResponse[];
  adataSource: IAllContainerResponse[];
  totalCount: number;
  expandedRowData: IExpandedRow;
}

export class MissingContainers extends React.Component<Record<string, object>, IMissingContainersState> {
  constructor(props = {}) {
    super(props);
    this.state = {
      loading: false,
      dataSource: [],
      udataSource: [],
      odataSource: [],
      mdataSource: [],
      adataSource: [],
      totalCount: 0,
      expandedRowData: {}
    };
  }

  componentDidMount(): void {
    // Fetch missing containers on component mount
    this.setState({
      loading: true
    });

    axios.all([
      axios.get('/api/v1/containers/missing'),
      axios.get('/api/v1/containers/unhealthy/UNDER_REPLICATED'),
      axios.get('/api/v1/containers/unhealthy/OVER_REPLICATED'),
      axios.get('/api/v1/containers/unhealthy/MIS_REPLICATED'),
      axios.get('/api/v1/containers/unhealthy/ALL_REPLICAS_UNHEALTHY'),
    ]).then(axios.spread((missingContainersResponse, underReplicatedResponse, overReplicatedResponse, misReplicatedResponse, allReplicatedResponse) => {
      
      const missingContainersResponse1: IMissingContainersResponse = missingContainersResponse.data;
      const totalCount = missingContainersResponse1.totalCount;
      const missingContainers: IMissingContainerResponse[] = missingContainersResponse1.containers;

      const underReplicatedResponse1: IAllReplicatedContainersResponse = underReplicatedResponse.data;
      const uContainers: IAllContainerResponse[] = underReplicatedResponse1.containers;
      
      const overReplicatedResponse1: IAllReplicatedContainersResponse = overReplicatedResponse.data;
      const oContainers: IAllContainerResponse[] = overReplicatedResponse1.containers;
      
      const misReplicatedResponse1: IAllReplicatedContainersResponse = misReplicatedResponse.data;
      const mContainers: IAllContainerResponse[] = misReplicatedResponse1.containers;

      const allReplicatedResponse1: IAllReplicatedContainersResponse = allReplicatedResponse.data;
      const aContainers: IAllContainerResponse[] = allReplicatedResponse1.containers;
      
      this.setState({
        loading: false,
        dataSource: missingContainers,
        udataSource: uContainers,
        odataSource: oContainers,
        mdataSource: mContainers,
        adataSource: aContainers,
        totalCount
      });
    })).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });
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

  onRowExpandClick = (expanded: boolean, record: IMissingContainerResponse) => {
    if (expanded) {
      this.setState(({expandedRowData}) => {
        const expandedRowState: IExpandedRowState = expandedRowData[record.containerID] ?
          Object.assign({}, expandedRowData[record.containerID], {loading: true}) :
          {containerId: record.containerID, loading: true, dataSource: [], totalCount: 0};
        return {
          expandedRowData: Object.assign({}, expandedRowData, {[record.containerID]: expandedRowState})
        };
      });
      axios.get(`/api/v1/containers/${record.containerID}/keys`).then(response => {
        const containerKeysResponse: IContainerKeysResponse = response.data;
        this.setState(({expandedRowData}) => {
          const expandedRowState: IExpandedRowState =
              Object.assign({}, expandedRowData[record.containerID],
                {loading: false, dataSource: containerKeysResponse.keys, totalCount: containerKeysResponse.totalCount});
          return {
            expandedRowData: Object.assign({}, expandedRowData, {[record.containerID]: expandedRowState})
          };
        });
      }).catch(error => {
        this.setState(({expandedRowData}) => {
          const expandedRowState: IExpandedRowState =
              Object.assign({}, expandedRowData[record.containerID],
                {loading: false});
          return {
            expandedRowData: Object.assign({}, expandedRowData, {[record.containerID]: expandedRowState})
          };
        });
        showDataFetchError(error.toString());
      });
    }
  };

  expandedRowRender = (record: IMissingContainerResponse) => {
    const {expandedRowData} = this.state;
    const containerId = record.containerID;
    if (expandedRowData[containerId]) {
      const containerKeys: IExpandedRowState = expandedRowData[containerId];
      const dataSource = containerKeys.dataSource.map(record => (
        {...record, uid: `${record.Volume}/${record.Bucket}/${record.Key}`}
      ));
      const paginationConfig: PaginationConfig = {
        showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} keys`
      };
      return (
        <Table
          loading={containerKeys.loading} dataSource={dataSource}
          columns={KEY_TABLE_COLUMNS} pagination={paginationConfig}
          rowKey='uid'/>
      );
    }

    return <div>Loading...</div>;
  };

  render() {
    const {dataSource, loading, udataSource, odataSource, mdataSource ,adataSource} = this.state;
    const paginationConfig: PaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} missing containers`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
      <div className='missing-containers-container'>
        <div className='page-header'>
          Containers
        </div>
        <div className='content-div'>
          <Tabs defaultActiveKey='1' onChange={this.onTabChange}>
            <TabPane key='1' tab="Missing">
              <Table
                expandRowByClick dataSource={dataSource} columns={COLUMNS}
                loading={loading}
                pagination={paginationConfig} rowKey='containerID'
                expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}/>
            </TabPane>
            <TabPane key='2' tab='Under-Replicated'>
              <Table
                expandRowByClick dataSource={udataSource} columns={UCOLUMNS}
                loading={loading}
                pagination={paginationConfig} rowKey='containerID'
                expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}/>
            </TabPane>
            <TabPane key='3' tab='Over-Replicated'>
              <Table
                expandRowByClick dataSource={odataSource} columns={OCOLUMNS}
                loading={loading}
                pagination={paginationConfig} rowKey='containerID'
                expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}/>
            </TabPane>
            <TabPane key='4' tab='Mis-Replicated'>
              <Table
                expandRowByClick dataSource={mdataSource} columns={MCOLUMNS}
                loading={loading}
                pagination={paginationConfig} rowKey='containerID'
                expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}/>
            </TabPane>
            <TabPane key='5' tab='All-Replicated'>
              <Table
                expandRowByClick dataSource={adataSource} columns={MCOLUMNS}
                loading={loading}
                pagination={paginationConfig} rowKey='containerID'
                expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}/>
            </TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}
