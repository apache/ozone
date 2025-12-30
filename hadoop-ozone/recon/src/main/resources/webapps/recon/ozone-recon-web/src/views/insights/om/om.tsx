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
import {Dropdown, Menu, Table, Tabs, Tooltip} from 'antd';
import {MenuProps} from 'antd/es/menu';
import {TablePaginationConfig} from 'antd/es/table';
import {FunnelPlotFilled, InfoCircleOutlined} from '@ant-design/icons';
import {ActionMeta, ValueType} from "react-select";
import CreatableSelect from "react-select/creatable";

import {ColumnSearch} from '@/utils/columnSearch';
import {byteToSize, showDataFetchError} from '@/utils/common';
import {AxiosGetHelper, cancelRequests} from '@/utils/axiosRequestHelper';
import {IOption} from "@/components/multiSelect/multiSelect";

import './om.less';
import { ReplicationInfo } from '@/v2/types/insights.types';


const size = filesize.partial({ standard: 'iec' });

let keysPendingExpanded: any = [];
interface IContainerResponse {
  containerId: number;
  mismatchMissingState: string;
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

const MISMATCH_TAB_COLUMNS = [
  {
    title: 'Container ID',
    dataIndex: 'containerId',
    key: 'containerId',
    width: '20%',
    isSearchable: true,

  },
  {
    title: 'Count Of Keys',
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

const OPEN_KEY_TAB_COLUMNS = [
  {
    title: 'Key Name',
    dataIndex: 'path',
    key: 'path',
    isSearchable: true
  },
  {
    title: 'Amount of data',
    dataIndex: 'size',
    key: 'size',
    render: (size: any) => size = byteToSize(size, 1)
  },
  {
    title: 'Path',
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
    title: 'Replication Type',
    dataIndex: 'replicationInfo',
    key: 'replicationtype',
    render: (replicationInfo: ReplicationInfo) => (
      <div>
        {replicationInfo.replicationType}
      </div>
    )
  },
  {
    title: 'Replication Factor',
    dataIndex: 'replicationInfo',
    key: 'replicationfactor',
    render: (replicationInfo: ReplicationInfo) => (
      <div>
        {
          (replicationInfo.replicationType === "RATIS")
          ? replicationInfo.replicationFactor
          : `${replicationInfo.codec}-${replicationInfo.data}-${replicationInfo.parity}`
        }
      </div>
    )
  }
];

const PENDING_TAB_COLUMNS = [
  {
    title: 'Key Name',
    dataIndex: 'fileName',
    key: 'fileName'
  },
  {
    title: 'Path',
    dataIndex: 'keyName',
    key: 'keyName',
    isSearchable: true,
  },
  {
    title: 'Total Data Size',
    dataIndex: 'dataSize',
    key: 'dataSize',
    render: (dataSize: any) => dataSize = byteToSize(dataSize, 1)
  },
  {
    title: 'Total Key Count',
    dataIndex: 'keyCount',
    key: 'keyCount',
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
    title: 'Count Of Keys',
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
        {pipelines && pipelines.map((pipeline: any) => (
          <div key={pipeline.id.id}>
            {pipeline.id.id}
          </div>
        ))}
      </div>
    )
  }
];

const PENDINGDIR_TAB_COLUMNS = [
  {
    title: 'Directory Name',
    dataIndex: 'key',
    isSearchable: true,
    key: 'key'
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
    title: 'Path',
    dataIndex: 'path',
    key: 'path',
    width: '450px'
  },
  {
    title: 'Data Size',
    dataIndex: 'size',
    key: 'size',
    render: (dataSize: any) => dataSize = byteToSize(dataSize, 1)
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
  deletedContainerKeysDataSource: [];
  mismatchMissingState: any;
  pendingDeleteDirDataSource: any[];
  activeTab: string;
  includeFso: boolean;
  includeNonFso: boolean;
  selectedLimit: IOption;
}

const LIMIT_OPTIONS: IOption[] = [
  {
    label: '1000',
    value: '1000'
  },
  {
    label: '5000',
    value: '5000'
  },
  {
    label: '10000',
    value: '10000'
  },
  {
    label: '20000',
    value: '20000'
  }
]

const INITIAL_LIMIT_OPTION = LIMIT_OPTIONS[0]

let cancelMismatchedEndpointSignal: AbortController;
let cancelOpenKeysSignal: AbortController;
let cancelDeletePendingSignal: AbortController;
let cancelDeletedKeysSignal: AbortController;
let cancelRowExpandSignal: AbortController;
let cancelDeletedPendingDirSignal: AbortController;

export class Om extends React.Component<Record<string, object>, IOmdbInsightsState> {

  constructor(props = {}) {
    super(props);
    this.addexistAtColumn();
    this.addfsoNonfsoKeyColumn();
    this.state = {
      loading: false,
      mismatchDataSource: [],
      openKeysDataSource: [],
      pendingDeleteKeyDataSource: [],
      deletedContainerKeysDataSource: [],
      pendingDeleteDirDataSource: [],
      mismatchMissingState: 'SCM',
      expandedRowData: {},
      activeTab: props.location.state ? props.location.state.activeTab : '1',
      includeFso: true,
      includeNonFso: false,
      selectedLimit: INITIAL_LIMIT_OPTION
    };
  }

  addexistAtColumn = () => {
    // Inside the class component to access the React internal state
    const existsAtColumn = {
      title: <span>
        <Dropdown
          overlay={
            <Menu onClick={this.handleExistsAtChange}>
              <Menu.Item key='OM'>
                OM
              </Menu.Item>
              <Menu.Item key='SCM'>
                SCM
              </Menu.Item>
            </Menu>} >
          <label> Exists at&nbsp;&nbsp;
            <FunnelPlotFilled />&nbsp;&nbsp;&nbsp;&nbsp;
          </label>
        </Dropdown>&nbsp;&nbsp;
        <label>
          <Tooltip placement='top' title={<span>{'SCM: Container exist at SCM but missing at OM.'}<br />
            {'OM: Container exist at OM but missing at SCM.'}</span>}>
            <InfoCircleOutlined />
          </Tooltip>
        </label>
      </span>,
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
    if (MISMATCH_TAB_COLUMNS.length > 0 && MISMATCH_TAB_COLUMNS[MISMATCH_TAB_COLUMNS.length - 1].key !== 'existsAt') {
      MISMATCH_TAB_COLUMNS.push(existsAtColumn);
    }
  };

  handleExistsAtChange: MenuProps["onClick"] = ({ key }) => {
    console.log('handleExistsAtChange', key);
    if (key === 'OM') {
      this.fetchMismatchContainers('SCM');
    }
    else {
      this.fetchMismatchContainers('OM');
    }
  };

  addfsoNonfsoKeyColumn = () => {
    // Inside the class component to access the React internal state
    const fsoNonfsoColumn = {
      title: <span>
        <Dropdown overlay={
          <Menu
            defaultSelectedKeys={['OM']}
            onClick={this.handlefsoNonfsoMenuChange}>
            <Menu.Item key='fso'>
              FSO
            </Menu.Item>
            <Menu.Item key='nonFso'>
              Non FSO
            </Menu.Item>
          </Menu>
        }>
          <label> Type&nbsp;&nbsp;
            <FunnelPlotFilled />
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
    if (OPEN_KEY_TAB_COLUMNS.length > 0 && OPEN_KEY_TAB_COLUMNS[OPEN_KEY_TAB_COLUMNS.length - 1].key !== 'type') {
      OPEN_KEY_TAB_COLUMNS.push(fsoNonfsoColumn);
    }
  };


  handlefsoNonfsoMenuChange: MenuProps["onClick"] = (e) => {
    if (e.key === 'fso') {
      this.fetchOpenKeys(true, false);
    }
    else {
      this.fetchOpenKeys(false, true);
    }
  };

  _loadData = () => {
    if (this.state.activeTab === '1') {
      this.fetchMismatchContainers(this.state.mismatchMissingState);
    } else if (this.state.activeTab === '2') {
      this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso);
    } else if (this.state.activeTab === '3') {
      keysPendingExpanded = [];
      this.fetchDeletePendingKeys();
    } else if (this.state.activeTab === '4') {
      this.fetchDeletedKeys();
    } else if (this.state.activeTab === '5') {
      this.fetchDeletePendingDir();
    }
  }

  componentDidMount(): void {
    this._loadData();
  };

  componentWillUnmount(): void {
    cancelMismatchedEndpointSignal && cancelMismatchedEndpointSignal.abort();
    cancelOpenKeysSignal && cancelOpenKeysSignal.abort();
    cancelDeletePendingSignal && cancelDeletePendingSignal.abort();
    cancelDeletedKeysSignal && cancelDeletedKeysSignal.abort();
    cancelRowExpandSignal && cancelRowExpandSignal.abort();
    cancelDeletedPendingDirSignal && cancelDeletedPendingDirSignal.abort();
  }

  fetchMismatchContainers = (mismatchMissingState: any) => {
    this.setState({
      loading: true,
      mismatchMissingState
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal,
      cancelDeletedPendingDirSignal
    ]);

    const mismatchEndpoint = `/api/v1/containers/mismatch?limit=${this.state.selectedLimit.value}&missingIn=${mismatchMissingState}`
    const { request, controller } = AxiosGetHelper(mismatchEndpoint, cancelMismatchedEndpointSignal)
    cancelMismatchedEndpointSignal = controller;
    request.then(mismatchContainersResponse => {
      const mismatchContainers: IContainerResponse[] = mismatchContainersResponse?.data?.containerDiscrepancyInfo && [];

      this.setState({
        loading: false,
        mismatchDataSource: mismatchContainers
      });

    }).catch(error => {
      this.setState({
        loading: false,
      });
      showDataFetchError(error);
    });
  };

  fetchOpenKeys = (includeFso: boolean, includeNonFso: boolean) => {
    this.setState({
      loading: true,
      includeFso,
      includeNonFso
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal,
      cancelDeletedPendingDirSignal
    ]);

    let openKeysEndpoint = `/api/v1/keys/open?includeFso=${includeFso}&includeNonFso=${includeNonFso}&limit=${this.state.selectedLimit.value}`;

    const { request, controller } = AxiosGetHelper(openKeysEndpoint, cancelOpenKeysSignal)
    cancelOpenKeysSignal = controller
    request.then(openKeysResponse => {
      const openKeys = openKeysResponse?.data ?? {"fso": []};
      let allopenKeysResponse: any[] = [];
      for (let key in openKeys) {
        if (Array.isArray(openKeys[key])) {
          openKeys[key] && openKeys[key].map((item: any) => (
            allopenKeysResponse.push({
              ...item,
              type: key
            })));
        }
      }
      this.setState({
        loading: false,
        openKeysDataSource: allopenKeysResponse,
      })

    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error);
    });

  };

  fetchDeletePendingKeys = () => {
    this.setState({
      loading: true
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal,
      cancelDeletedPendingDirSignal
    ]);

    keysPendingExpanded = [];
    let deletePendingKeysEndpoint = `/api/v1/keys/deletePending?limit=${this.state.selectedLimit.value}`;

    const { request, controller } = AxiosGetHelper(deletePendingKeysEndpoint, cancelDeletePendingSignal);
    cancelDeletePendingSignal = controller;

    request.then(deletePendingKeysResponse => {
      const deletePendingKeys = deletePendingKeysResponse?.data?.deletedKeyInfo ?? [];
      //Use Summation Logic iterate through all object and find sum of all datasize
      let deletedKeyInfoData = [];
      deletedKeyInfoData = deletePendingKeys && deletePendingKeys.flatMap((infoObject: any) => {
        const { omKeyInfoList } = infoObject;
        keysPendingExpanded.push(infoObject);
        let count = 0;
        let item = omKeyInfoList && omKeyInfoList.reduce((obj: any, item: any) => {
          const { dataSize } = item;
          const newDataSize = obj.dataSize + dataSize;
          count = count + 1;
          return { ...item, dataSize: newDataSize };
        }, { 'dataSize': 0 });

        return {
          'dataSize': item.dataSize,
          'fileName': item.fileName,
          'keyName': item.keyName,
          'path': item.path,
          'keyCount': count
        }
      });

      this.setState({
        loading: false,
        pendingDeleteKeyDataSource: deletedKeyInfoData
      });

    }).catch(error => {
      this.setState({
        loading: false,
      });
      showDataFetchError(error);
    });
  };

  expandedKey = (record: any) => {
    const filteredData = keysPendingExpanded && keysPendingExpanded.flatMap((info: any) =>
      info.omKeyInfoList && info.omKeyInfoList.filter((item: any) => item.keyName === record.keyName)
    )
    const columns = [{
      title: 'Data Size',
      dataIndex: 'dataSize',
      key: 'dataSize',
      render: (dataSize: any) => dataSize = dataSize > 0 ? byteToSize(dataSize, 1) : dataSize
    },
    {
      title: 'Replicated Data Size',
      dataIndex: 'replicatedSize',
      key: 'replicatedSize',
      render: (replicatedSize: any) => replicatedSize = replicatedSize > 0 ? byteToSize(replicatedSize, 1) : replicatedSize
    },
    {
      title: 'Creation Time',
      dataIndex: 'creationTime',
      key: 'creationTime',
      render: (creationTime: number) => {
        return creationTime > 0 ? moment(creationTime).format('ll LTS') : 'NA';
      }
    },
    {
      title: 'Modification Time',
      dataIndex: 'modificationTime',
      key: 'modificationTime',
      render: (modificationTime: number) => {
        return modificationTime > 0 ? moment(modificationTime).format('ll LTS') : 'NA';
      }
    }
    ]
    return (
      <Table
        columns={columns}
        dataSource={filteredData}
        pagination={true}
        rowKey='dataSize'
        locale={{ filterTitle: '' }}
      />
    );
  }

  fetchDeletedKeys = () => {
    this.setState({
      loading: true
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal,
      cancelDeletedPendingDirSignal
    ]);

    const deletedKeysEndpoint = `/api/v1/containers/mismatch/deleted?limit=${this.state.selectedLimit.value}`;
    const { request, controller } = AxiosGetHelper(deletedKeysEndpoint, cancelDeletedKeysSignal);
    cancelDeletedKeysSignal = controller
    request.then(deletedKeysResponse => {
      let deletedContainerKeys = [];
      deletedContainerKeys = deletedKeysResponse?.data?.containers ?? [];
      this.setState({
        loading: false,
        deletedContainerKeysDataSource: deletedContainerKeys
      })
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error);
    });
  };

  // Pending Delete Directories
  fetchDeletePendingDir = () => {
    this.setState({
      loading: true
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal,
      cancelDeletedPendingDirSignal
    ]);

    const DELETE_PENDING_DIR_ENDPOINT = `/api/v1/keys/deletePending/dirs?limit=${this.state.selectedLimit.value}`;
    const { request, controller } = AxiosGetHelper(DELETE_PENDING_DIR_ENDPOINT, cancelDeletedPendingDirSignal);
    cancelDeletedPendingDirSignal = controller
    request.then(deletePendingDirResponse => {
      let deletedDirInfo = [];
      deletedDirInfo = deletePendingDirResponse?.data?.deletedDirInfo ?? [];
      this.setState({
        loading: false,
        pendingDeleteDirDataSource: deletedDirInfo
      });
    }).catch(error => {
      this.setState({
        loading: false,
      });
      showDataFetchError(error);
    });
  };


  changeTab = (activeKey: any) => {
    this.setState({
      activeTab: activeKey,
      mismatchDataSource: [],
      openKeysDataSource: [],
      pendingDeleteKeyDataSource: [],
      deletedContainerKeysDataSource: [],
      expandedRowData: {},
      mismatchMissingState: 'SCM',
      includeFso: true,
      includeNonFso: false,
      selectedLimit: INITIAL_LIMIT_OPTION
    }, () => {
      if (activeKey === '2') {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso);
      } else if (activeKey === '3') {
        keysPendingExpanded = [];
        this.fetchDeletePendingKeys();
      } else if (activeKey === '4') {
        this.fetchDeletedKeys();
      } else if (activeKey === '5') {
        this.fetchDeletePendingDir();
      }
      else {
        this.fetchMismatchContainers(this.state.mismatchMissingState);
      }
    })
  };

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
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

      const { request, controller } = AxiosGetHelper(`/api/v1/containers/${record.containerId}/keys`, cancelRowExpandSignal);
      cancelRowExpandSignal = controller;

      request.then(response => {
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
        showDataFetchError(error);
      });
    }
    else {
      cancelRowExpandSignal && cancelRowExpandSignal.abort()
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
      const paginationConfig: TablePaginationConfig = {
        showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} keys`
      };
      return (
        <Table
          loading={containerKeys.loading} dataSource={dataSource}
          columns={KEY_TABLE_COLUMNS} pagination={paginationConfig}
          rowKey='uid'
          locale={{ filterTitle: "" }} />
      );
    }
    return <div>Loading...</div>;
  };

  searchMismatchColumn = () => {
    return MISMATCH_TAB_COLUMNS.reduce<any[]>((filtered, column) => {
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

  searchOpenKeyColumn = () => {
    return OPEN_KEY_TAB_COLUMNS.reduce<any[]>((filtered, column) => {
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

  searchKeysPendingColumn = () => {
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

  searchDeletedKeyColumn = () => {
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

  searchDirPendingColumn = () => {
    return PENDINGDIR_TAB_COLUMNS.reduce<any[]>((filtered, column) => {
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

  _handleLimitChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const selectedLimit = (selected as IOption)
    this.setState({
      selectedLimit
    }, this._loadData);
  }

  _onCreateOption = (created: string) => {
    // Check that it's a numeric and non-negative
    if (parseInt(created)) {
      const createdOption: IOption = {
        label: created,
        value: created
      }
      this.setState({
        selectedLimit: createdOption
      }, this._loadData);
    } else {
      console.log('Not a valid option')
    }
  }

  render() {
    const { mismatchDataSource, loading, openKeysDataSource, pendingDeleteKeyDataSource, deletedContainerKeysDataSource, pendingDeleteDirDataSource, selectedLimit } = this.state;

    const paginationConfig: TablePaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total}`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange,
    };

    const generateMismatchTable = (dataSource: any) => {
      return <Table
        expandable={{
          expandRowByClick: true,
          expandedRowRender: this.expandedRowRender,
          onExpand: this.onRowExpandClick
        }}
        dataSource={dataSource}
        columns={this.searchMismatchColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='containerId'
        locale={{ filterTitle: "" }} />
    }

    const generateOpenKeyTable = (dataSource: any) => {
      return <Table
        expandable={{
          expandRowByClick: true,
        }}
        dataSource={dataSource}
        columns={this.searchOpenKeyColumn()}
        loading={loading} rowKey='key'
        pagination={paginationConfig}
        locale={{ filterTitle: "" }} />
    }

    const generateKeysPendingTable = (dataSource: any) => {
      return <Table
        expandable={{
          expandRowByClick: true,
          expandedRowRender: this.expandedKey
        }}
        dataSource={dataSource}
        columns={this.searchKeysPendingColumn()}
        loading={loading}
        pagination={paginationConfig}
        rowKey='keyName' />
    }

    const generateDeletedKeysTable = (dataSource: any) => {
      return <Table
        expandable={{
          expandRowByClick: true,
          expandedRowRender: this.expandedRowRender,
          onExpand: this.onRowExpandClick
        }}
        dataSource={dataSource}
        columns={this.searchDeletedKeyColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='containerId'
        locale={{ filterTitle: "" }}
      />
    }

    const generateDirPendingTable = (dataSource: any) => {
      return <Table
        expandable={{ expandRowByClick: true }}
        dataSource={dataSource}
        columns={this.searchDirPendingColumn()}
        loading={loading}
        pagination={paginationConfig}
        rowKey='key'
      />
    }

    return (
      <div>
        <div className='page-header'>
          OM DB Insights
        </div>
        <div className='content-div'>
          <div className='limit-block'>
            <CreatableSelect
              className='multi-select-container'
              isClearable={false}
              isDisabled={loading}
              isLoading={loading}
              onChange={this._handleLimitChange}
              onCreateOption={this._onCreateOption}
              isValidNewOption={(input, value, _option) => {
                // Only number will be accepted
                return !isNaN(parseInt(input))
              }}
              options={LIMIT_OPTIONS}
              hideSelectedOptions={false}
              value={selectedLimit}
              createOptionPosition='last'
              formatCreateLabel={(input) => {
                return `new limit... ${input}`
              }}
            /> Limit
          </div>
          <Tabs defaultActiveKey={this.state.activeTab} onChange={this.changeTab}>
            <Tabs.TabPane key='1' tab='Container Mismatch Info'>
              {generateMismatchTable(mismatchDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane key='2' tab='Open Keys'>
              {generateOpenKeyTable(openKeysDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane key='3' tab={(
              <label>Keys Pending for Deletion&nbsp;&nbsp;
                <Tooltip placement='top' title="Keys that are pending for deletion.">
                  <InfoCircleOutlined />
                </Tooltip>
              </label>
            )}>
              {generateKeysPendingTable(pendingDeleteKeyDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane key='4' tab={(
              <label>Deleted Container Keys&nbsp;&nbsp;
                <Tooltip placement='top' title={'Keys mapped to Containers in DELETED state SCM.'}>
                  <InfoCircleOutlined />
                </Tooltip>
              </label>
            )}>
              {generateDeletedKeysTable(deletedContainerKeysDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane key='5' tab={(
              <label>Directories Pending for Deletion&nbsp;&nbsp;
                <Tooltip placement='top' title="Directories that are pending for deletion.">
                  <InfoCircleOutlined />
                </Tooltip>
              </label>
            )}>
              {generateDirPendingTable(pendingDeleteDirDataSource)}
            </Tabs.TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}
