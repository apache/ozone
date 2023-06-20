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
import { Table, Tabs, Menu, Dropdown, Icon, Tooltip } from 'antd';
import { PaginationConfig } from 'antd/lib/pagination';
import filesize from 'filesize';
import moment from 'moment';
import { showDataFetchError } from 'utils/common';
import './om.less';
import { ColumnSearch } from 'utils/columnSearch';
import { Link } from 'react-router-dom';


const size = filesize.partial({ standard: 'iec' });
const { TabPane } = Tabs;
interface IContainerResponse {
  containerId: number;
  containerState: string;
  OMContainerState: string;
  SCMContainerState: string;
  existsAt: string;
  pipelines: string[];
  numberOfKeys: number;
}

interface IKeyLevelResponse {
  path: string;
  keyState: string;
  inStateSince: number;
  size: number;
  replicatedSize: number;
  unreplicatedSize: number;
  replicationType: string;
}

interface IDeleteKeyResponse {
  objectID: number;
  updateID: number;
  parentObjectID: number;
  volumeName: string;
  bucketName: string;
  keyName: string;
  dataSize: number;
  path: string;
  replicatedTotal: number;
  unreplicatedTotal: number;
  omKeyInfoList: IDeleteKeyResponse[];
}

interface IContainersResponse {
  containers: IContainerResponse[];
}

interface IKeysResponse {
  entities: IKeyLevelResponse[];
}
interface IDeleteKeysResponse {
  deletedkeyinfo: IDeleteKeyResponse[];
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
    dataIndex: 'containerId',
    key: 'containerId',
    width: '20%',
    isSearchable: true,

  },
  {
    title: 'Keys',
    dataIndex: 'numberOfKeys',
    key: 'numberOfKeys',
    sorter: (a: IContainerResponse, b: IContainerResponse) => a.numberOfKeys - b.numberOfKeys
  },
  {
    title: 'Pipelines',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: any) => (
      <div>
        {pipelines && pipelines.map(pipeline => (
          <div key={pipeline.id.id}>
            {pipeline.id.id}
          </div>
        ))}
      </div>
    )
  }
];

const OPEN_KEY_COLUMNS = [
  {
    title: 'Path',
    dataIndex: 'path',
    key: 'path',
    isSearchable: true,
  },
  {
    title: 'Amount of data',
    dataIndex: 'size',
    key: 'size',
  },
  {
    title: 'Key',
    dataIndex: 'key',
    key: 'key',
    width: '270px'
  },
  {
    title: 'In state since',
    dataIndex: 'inStateSince',
    key: 'inStateSince',
    render: (inStateSince: number) => {
      return inStateSince > 0 ? moment(inStateSince).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Replication Factor',
    dataIndex: 'replicationInfo',
    key: 'replicationfactor',
    render: (replicationInfo: any) => (
      <div>
        {
          <div >
            {Object.values(replicationInfo)[0]}
          </div>
        }
      </div>
    )
  },
  {
    title: 'Required Nodes',
    dataIndex: 'replicationInfo',
    key: 'replicationInfo',
    render: (replicationInfo: any) => (
      <div>
        {
          <div >
            {Object.values(replicationInfo)[1]}
          </div>
        }
      </div>
    )
  },
  {
    title: 'Replication Type',
    dataIndex: 'replicationInfo',
    key: 'replicationtype',
    render: (replicationInfo: any) => (
      <div>
        {
          <div >
            {Object.values(replicationInfo)[2]}
          </div>
        }
      </div>
    )
  }

];

const PENDING_TAB_COLUMNS = [
  {
    title: 'Key Name',
    dataIndex: 'keyName',
    key: 'keyName'
  },
  {
    title: 'Path',
    dataIndex: 'path',
    key: 'path',
    isSearchable: true,
  },
  {
    title: 'Amount of data',
    dataIndex: 'dataSize',
    key: 'dataSize',
  }
];

const DELETED_TAB_COLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerId',
    key: 'containerId',
    width: '20%',
    isSearchable: true
  },
  {
    title: 'Keys',
    dataIndex: 'numberOfKeys',
    key: 'numberOfKeys',
    sorter: (a: IContainerResponse, b: IContainerResponse) => a.numberOfKeys - b.numberOfKeys
  },
  {
    title: 'Pipelines',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: any) => (
      <div>
        {pipelines && pipelines.map(pipeline => (
          <div key={pipeline.id.id}>
            {pipeline.id.id}
          </div>
        ))}
      </div>
    )
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

interface IOmdbInsightsState {
  loading: boolean;
  mismatchDataSource: IContainerResponse[];
  openKeysDataSource: IKeyLevelResponse[];
  pendingDeleteKeyDataSource: any[];
  expandedRowData: IExpandedRow;
  scmKeyDataSource: [];
  prevKeyMismatch: number;
  containerState: any;
  prevKeyOpen: string;
  prevKeyDeleted: number;
  prevKeyDeletePending: string;
  activeTab: string;
  currentPage: number;
  DEFAULT_LIMIT: number,
  clickable: boolean;
  includeFso: boolean;
  includeNonFso: boolean;
}

export class Om extends React.Component<Record<string, object>, IOmdbInsightsState> {

  constructor(props = {}) {
    super(props);
    this.addStateColumn();
    this.addOpenKeyColumn();
    this.state = {
      loading: false,
      mismatchDataSource: [],
      openKeysDataSource: [],
      pendingDeleteKeyDataSource: [],
      scmKeyDataSource: [],
      prevKeyMismatch: 0,
      containerState: 'SCM',
      prevKeyOpen: "",
      prevKeyDeletePending: "",
      prevKeyDeleted: 0,
      expandedRowData: {},
      activeTab: '',
      currentPage: 1,
      DEFAULT_LIMIT: 10,
      clickable: true,
      includeFso: true,
      includeNonFso: false,
    };
  }

  addStateColumn = () => {
    // Inside the class component to access the React internal state
    const existsAtColumn = {
      title: <span>
        <Dropdown overlay={this.stateMenu} >
          <label> Exists at&nbsp;&nbsp;
            <Icon type="funnel-plot" theme="filled" />
          </label>
        </Dropdown></span>,
      dataIndex: 'existsAt',
      key: 'existsAt',
      isVisible: true,
      render: (existsAt: any) => {
        return (
          <div key={existsAt}>
            {existsAt}
          </div>

        );
      }
    }
    if (CONTAINER_TAB_COLUMNS.length > 0 && CONTAINER_TAB_COLUMNS[CONTAINER_TAB_COLUMNS.length - 1].key !== 'existsAt') {
      CONTAINER_TAB_COLUMNS.push(existsAtColumn);
    }
  };

  stateMenu = () => (
    <Menu
      // defaultSelectedKeys={this.state.containerState}
      onClick={e => this.handleStateMenuChange(e)}
    >
      <Menu.Item key='OM'>
        OM
      </Menu.Item>
      <Menu.Item key='SCM'>
        SCM
      </Menu.Item>
    </Menu>
  );

  handleStateMenuChange = (e: any) => {
    if (e.key === 'OM') {
      this.setState(() => ({
        containerState: 'SCM',
        prevKeyMismatch: 0,
        prevKeyOpen: "",
        prevKeyDeletePending: "",
        prevKeyDeleted: 0,
      }), () => {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.containerState)
      })
    }
    else {
      //start
      this.setState(() => ({
        containerState: 'OM',
        prevKeyMismatch: 0,
        prevKeyOpen: "",
        prevKeyDeletePending: "",
        prevKeyDeleted: 0,
      }), () => {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.containerState)
      })
    }
  };

  addOpenKeyColumn = () => {
    // Inside the class component to access the React internal state
    const typeColumn = {
      title: <span>
        <Dropdown overlay={this.typeMenu} >
          <label> Type&nbsp;&nbsp;
            <Icon type="funnel-plot" theme="filled" />
          </label>
        </Dropdown></span>,
      dataIndex: 'type',
      key: 'type',
      isVisible: true,

      render: (type: any) => {
        return (
          <div key={type}>
            {type}
          </div>
        );
      }
    }
    if (OPEN_KEY_COLUMNS.length > 0 && OPEN_KEY_COLUMNS[OPEN_KEY_COLUMNS.length - 1].key !== 'type') {
      OPEN_KEY_COLUMNS.push(typeColumn);
    }
  };

  typeMenu = () => (
    <Menu
      defaultSelectedKeys={['OM']}
      onClick={e => this.handleFsoNonFsoMenuChange(e)}
    >
      <Menu.Item key='fso'>
        FSO
      </Menu.Item>
      <Menu.Item key='nonFso'>
        Non FSO
      </Menu.Item>
    </Menu>
  );

  handleFsoNonFsoMenuChange = (e: any) => {
    if (e.key === 'fso') {
      this.setState(() => ({
        includeFso: true,
        includeNonFso: false,
        prevKeyOpen: "",
        prevKeyMismatch: 0,
        prevKeyDeletePending: "",
        prevKeyDeleted: 0,
      }), () => {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
      })
    }
    else {
      //start
      this.setState(() => ({
        includeFso: false,
        includeNonFso: true,
        prevKeyOpen: "",
        prevKeyMismatch: 0,
        prevKeyDeletePending: "",
        prevKeyDeleted: 0,
      }), () => {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen)
      })
    }
  };

  componentDidMount(): void {
    // Fetch mismatch containers on component mount
    this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.containerState);
  };

  fetchMismatchContainers = (limit: number, prevKeyMismatch: number, containerState: any) => {
    this.setState({
      loading: true,
      prevKeyOpen: "",
      prevKeyDeletePending: "",
      prevKeyDeleted: 0,
      clickable: true
    });
    const mismatchEndpoint = `/api/v1/containers/mismatch?limit=${limit}&prevKey=${prevKeyMismatch}&missingIn=${containerState}`
    axios.get(mismatchEndpoint).then(mismatchContainersResponse => {
      const mismatchContainers: IContainerResponse[] = mismatchContainersResponse && mismatchContainersResponse.data && mismatchContainersResponse.data.containerDiscrepancyInfo;

      if (mismatchContainersResponse && mismatchContainersResponse.data && mismatchContainersResponse.data.lastKey === null) {
        this.setState({
          loading: false,
          clickable: false,
        })
      }
      else {
        this.setState({
          loading: false,
          prevKeyMismatch: mismatchContainersResponse && mismatchContainersResponse.data && mismatchContainersResponse.data.lastKey,
          mismatchDataSource: mismatchContainers,
        });
      }
    }).catch(error => {
      this.setState({
        loading: false,
      });
    });
  };

  fetchOpenKeys = (includeFso: boolean, includeNonFso: boolean, limit: number, prevKeyOpen: string) => {
    this.setState({
      loading: true,
      prevKeyMismatch: 0,
      prevKeyDeletePending: "",
      prevKeyDeleted: 0,
      clickable: true
    });

    let openKeysEndpoint;
    if (prevKeyOpen === "") {
      openKeysEndpoint = `/api/v1/keys/open?includeFso=${includeFso}&includeNonFso=${includeNonFso}&limit=${limit}&prevKey`;
    }
    else {
      openKeysEndpoint = `/api/v1/keys/open?includeFso=${includeFso}&includeNonFso=${includeNonFso}&limit=${limit}&prevKey=${prevKeyOpen}`;
    }

    axios.get(openKeysEndpoint).then(openKeysResponse => {
      const openKeys = openKeysResponse && openKeysResponse.data;
      //&& (openKeysResponse.data.fso || openKeysResponse.data.nonFSO);
      let allopenKeysResponse: any[] = [];
      for (let key in openKeys) {
        if (Array.isArray(openKeys[key])) {
          openKeys[key] && openKeys[key].map((item: any) => (allopenKeysResponse.push({ ...item, type: key })));
        }
      }
      if (openKeysResponse && openKeysResponse.data && openKeysResponse.data.lastKey === "") {
        this.setState({
          loading: false,
          clickable: false,
        })
      }
      else {
        this.setState({
          loading: false,
          prevKeyOpen: openKeysResponse && openKeysResponse.data && openKeysResponse.data.lastKey,
          openKeysDataSource: allopenKeysResponse,
        })
      };
    }).catch(error => {
      this.setState({
        loading: false,
        clickable: false,
      });
    });

  };

  fetchDeletePendingKeys = (limit: number, prevKeyDeletePending: string) => {
    this.setState({
      loading: true,
      prevKeyOpen: "",
      prevKeyMismatch: 0,
      prevKeyDeleted: 0,
      clickable: true
    });
    let deletePendingKeysEndpoint
    if (prevKeyDeletePending === "") {
      deletePendingKeysEndpoint = `/api/v1/keys/deletePending?limit=${limit}&prevKey`
    }
    else {
      deletePendingKeysEndpoint = `/api/v1/keys/deletePending?limit=${limit}&prevKey=${prevKeyDeletePending}`
    }
    axios.get(deletePendingKeysEndpoint).then(deletePendingKeysResponse => {
      const deletePendingKeys = deletePendingKeysResponse && deletePendingKeysResponse.data && deletePendingKeysResponse.data.deletedKeyInfo;
      // const deletePendingKey = deletePendingKeys && deletePendingKeys.map(item => item.omKeyInfoList);
      const data = deletePendingKeys.flatMap((info) =>
        info.omKeyInfoList.map((item) => ({
          dataSize: item.dataSize,
          keyName: item.keyName,
          path: item.path
        })))
      if (deletePendingKeysResponse && deletePendingKeysResponse.data && deletePendingKeysResponse.data.lastKey === "") {
        this.setState({
          loading: false,
          clickable: false,
        })
      }
      else {
        this.setState({
          loading: false,
          prevKeyDeletePending: deletePendingKeysResponse && deletePendingKeysResponse.data && deletePendingKeysResponse.data.lastKey,
          pendingDeleteKeyDataSource: data
        });
      }
    }).catch(error => {
      this.setState({
        loading: false,
        clickable: false,
      });
      //showDataFetchError(error.toString());
    });
  };

  fetchDeletedSCMKeys = (limit: number, prevKeyDeleted: number) => {
    this.setState({
      loading: true,
      prevKeyOpen: "",
      prevKeyDeletePending: "",
      prevKeyMismatch: 0,
      clickable: true
    });
    const deletedSCMKeysEndpoint = `/api/v1/containers/mismatch/deleted?limit=${limit}&prevKey=${prevKeyDeleted}`
    axios.get(deletedSCMKeysEndpoint).then(deletedSCMKeysResponse => {
      const deletedSCMKeys = deletedSCMKeysResponse && deletedSCMKeysResponse.data && deletedSCMKeysResponse.data.containers;
      if (deletedSCMKeysResponse && deletedSCMKeysResponse.data && deletedSCMKeysResponse.data.lastKey === null) {
        this.setState({
          loading: false,
          clickable: false,
        })
      }
      else {
        this.setState({
          loading: false,
          prevKeyDeleted: deletedSCMKeysResponse && deletedSCMKeysResponse.data && deletedSCMKeysResponse.data.lastKey,
          scmKeyDataSource: deletedSCMKeys

        })
      };
    }).catch(error => {
      //alert("No More Further Records.");
      this.setState({
        loading: false,
        clickable: false,
      });
      //showDataFetchError(error.toString());
    });
  };

  changeTab = (activeKey: any) => {
    this.setState({
      activeTab: activeKey,
      currentPage: 1,
    }, () => {
      if (activeKey === '2') {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
      } else if (activeKey === '3') {
        this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
      } else if (activeKey === '4') {
        this.fetchDeletedSCMKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
      }
      else {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.containerState);
      }
    })
  };

  fetchPreviousRecords = () => {
    // To Call API for Page Level for each page
    if (this.state.activeTab === '2') {
      this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
    } else if (this.state.activeTab === '3') {
      this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
    } else if (this.state.activeTab === '4') {
      this.fetchDeletedSCMKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
    }
    else {
      this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.nextKeyMismatch, this.state.containerState);
    }
  };

  fetchNextRecords = () => {
    // To Call API for Next Records to fetch
    if (this.state.activeTab === '2') {
      this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
    } else if (this.state.activeTab === '3') {
      this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
    } else if (this.state.activeTab === '4') {
      this.fetchDeletedSCMKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
    }
    else {
      this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.containerState);
    }
  };

  itemRender = (_: any, type: string, originalElement: any) => {
    if (type === 'prev') {
      return <div>{<Link to="/Om" onClick={this.fetchPreviousRecords}> Prev</Link>}</div>;
    }
    if (type === 'next') {
      return <div> {this.state.clickable ? <Link to="/Om" onClick={this.fetchNextRecords}> {'>>'} </Link> : <Link to="/Om" style={{ pointerEvents: 'none' }}>No More Further Records</Link>}</div>;
    }
    return originalElement;
  };

  onShowSizeChange = (current: number, pageSize: number) => {
    // To Call API for Page Level for each page
    let new2 = 0;
    new2 = ((pageSize * current) - pageSize) + 1;
    if (this.state.activeTab === '2') {

      this.setState({
        DEFAULT_LIMIT: pageSize,
        currentPage: current,
      }, () => {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, new2 === 1 ? 0 : new2);
      });
    }
    else if (this.state.activeTab === '3') {
      this.setState({
        DEFAULT_LIMIT: pageSize,
        currentPage: current
      }, () => {
        this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, new2 === 1 ? 0 : new2);
      })
    }
    else if (this.state.activeTab === '4') {
      this.setState({
        DEFAULT_LIMIT: pageSize,
        currentPage: current,
      }, () => {
        this.fetchDeletedSCMKeys(this.state.DEFAULT_LIMIT, new2 === 1 ? 0 : new2);
      })
    }
    else {
      this.setState({
        DEFAULT_LIMIT: pageSize,
        currentPage: current,
      }, () => {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, new2 === 1 ? 0 : new2, this.state.containerState);
      });
    }
    //current=1;
  };

  onRowExpandClick = (expanded: boolean, record: IContainerResponse) => {
    if (expanded) {
      this.setState(({ expandedRowData }) => {
        const expandedRowState: IExpandedRowState = expandedRowData[record.containerId] ?
          Object.assign({}, expandedRowData[record.containerId], { loading: true }) :
          { containerId: record.containerId, loading: true, dataSource: [], totalCount: 0 };
        return {
          expandedRowData: Object.assign({}, expandedRowData, { [record.containerId]: expandedRowState })
        };
      });
      axios.get(`/api/v1/containers/${record.containerId}/keys`).then(response => {
        const containerKeysResponse: IContainerKeysResponse = response.data;
        this.setState(({ expandedRowData }) => {
          const expandedRowState: IExpandedRowState =
            Object.assign({}, expandedRowData[record.containerId],
              { loading: false, dataSource: containerKeysResponse.keys, totalCount: containerKeysResponse.totalCount });
          return {
            expandedRowData: Object.assign({}, expandedRowData, { [record.containerId]: expandedRowState })
          };
        });
      }).catch(error => {
        this.setState(({ expandedRowData }) => {
          const expandedRowState: IExpandedRowState =
            Object.assign({}, expandedRowData[record.containerId],
              { loading: false });
          return {
            expandedRowData: Object.assign({}, expandedRowData, { [record.containerId]: expandedRowState })
          };
        });
        showDataFetchError(error.toString());
      });
    }
  };

  expandedRowRender = (record: IContainerResponse) => {
    const { expandedRowData } = this.state;
    const containerId = record.containerId;
    if (expandedRowData[containerId]) {
      const containerKeys: IExpandedRowState = expandedRowData[containerId];
      const dataSource = containerKeys && containerKeys.dataSource && containerKeys.dataSource.map(record => (
        { ...record, uid: `${record.Volume}/${record.Bucket}/${record.Key}` }
      ));
      const paginationConfig: PaginationConfig = {
        showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} keys`
      };
      return (
        <Table
          loading={containerKeys.loading} dataSource={dataSource}
          columns={KEY_TABLE_COLUMNS} pagination={paginationConfig}
          rowKey='uid' />
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

  searchKeyColumn = () => {
    return OPEN_KEY_COLUMNS.reduce<any[]>((filtered, column) => {
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

  searchPendingColumn = () => {
    return PENDING_TAB_COLUMNS.reduce<any[]>((filtered, column) => {
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

  searchscmKeyColumn = () => {
    return DELETED_TAB_COLUMNS.reduce<any[]>((filtered, column) => {
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
    const { mismatchDataSource, loading, openKeysDataSource, pendingDeleteKeyDataSource, scmKeyDataSource } = this.state;

    const paginationConfig: PaginationConfig = {
      //showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} containers`,
      defaultPageSize: this.state.DEFAULT_LIMIT,
      pageSizeOptions: ['10', '20', '30', '50'],
      showQuickJumper: true,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange,
      //current:this.state.currentPage,
      //onChange: this.onChangePagination,
      itemRender: this.itemRender
    };

    const generateTable = (dataSource: any) => {
      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='containerId'
        expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick} />
    }

    const generateKeyTable = (dataSource: any) => {
      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchKeyColumn()}
        loading={loading}
        pagination={paginationConfig} />
    }

    const generatePendingTable = (dataSource: any) => {

      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchPendingColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='keyName' />
    }

    const generateDeletedTable = (dataSource: any) => {
      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchscmKeyColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='containerId'
        expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}
      />
    }

    return (
      <div className='missing-containers-container'>
        <div className='page-header'>
          OM DB Insights
        </div>
        <div className='content-div'>
          <Tabs defaultActiveKey='1' onChange={this.changeTab}>
            <TabPane key='1' tab={`Container Mismatch Info${(mismatchDataSource && mismatchDataSource.length > 0) ? ` (${mismatchDataSource.length})` : ''}`}>
              {generateTable(mismatchDataSource)}
            </TabPane>
            <TabPane key='2' tab={`Open Keys${(openKeysDataSource && openKeysDataSource.length > 0) ? ` (${openKeysDataSource.length})` : ''}`}>
              {generateKeyTable(openKeysDataSource)}
            </TabPane>
            <TabPane key='3'
              tab={<label>Keys Pending for Deletion{(pendingDeleteKeyDataSource && pendingDeleteKeyDataSource.length > 0) ? ` (${pendingDeleteKeyDataSource.length})` : ''}
                <Tooltip placement='top' title="Keys and Directories that are pending for deletion">
                  <Icon type='info-circle' />
                </Tooltip>
              </label>
              }>
              {generatePendingTable(pendingDeleteKeyDataSource)}
            </TabPane>
            <TabPane key='4'
              tab={<label>Deleted Container Keys {(scmKeyDataSource && scmKeyDataSource.length > 0) ? ` (${scmKeyDataSource.length})` : ''}
                <Tooltip placement='top' title="Containers in DELETED state in SCM ">
                  <Icon type='info-circle' />
                </Tooltip>
              </label>
              }>
              {generateDeletedTable(scmKeyDataSource)}
            </TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}