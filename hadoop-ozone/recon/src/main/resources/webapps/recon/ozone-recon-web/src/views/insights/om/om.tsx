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
import {
  Table,
  Tabs,
  Menu,
  Dropdown,
  Tooltip
} from 'antd';
import { TablePaginationConfig } from 'antd/es/table';
import { FunnelPlotFilled, InfoCircleOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';

import { showDataFetchError, byteToSize } from '@/utils/common';
import { ColumnSearch } from '@/utils/columnSearch';
import { AxiosGetHelper, cancelRequests } from '@/utils/axiosRequestHelper';

import './om.less';


const size = filesize.partial({ standard: 'iec' });

//Previous Key Need to store respective Lastkey of each API
let mismatchPrevKeyList = [0];
let openPrevKeyList = [''];
let keysPendingPrevList = [''];
let deletedKeysPrevList = [0];
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
    title: 'Key',
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
  prevKeyMismatch: number;
  mismatchMissingState: any;
  prevKeyOpen: string;
  prevKeyDeleted: number;
  prevKeyDeletePending: string;
  activeTab: string;
  DEFAULT_LIMIT: number,
  nextClickable: boolean;
  includeFso: boolean;
  includeNonFso: boolean;
  prevClickable: boolean
}

let cancelMismatchedEndpointSignal: AbortController;
let cancelOpenKeysSignal: AbortController;
let cancelDeletePendingSignal: AbortController;
let cancelDeletedKeysSignal: AbortController;
let cancelRowExpandSignal: AbortController;

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
      prevKeyMismatch: 0,
      mismatchMissingState: 'SCM',
      prevKeyOpen: "",
      prevKeyDeletePending: "",
      prevKeyDeleted: 0,
      expandedRowData: {},
      activeTab: props.location.state ? props.location.state.activeTab : '1',
      DEFAULT_LIMIT: 10,
      nextClickable: true,
      includeFso: true,
      includeNonFso: false,
      prevClickable: false
    };
  }

  addexistAtColumn = () => {
    // Inside the class component to access the React internal state
    const existsAtColumn = {
      title: <span>
        <Dropdown overlay={this.existAtScmOmMenu} >
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

  existAtScmOmMenu = () => (
    <Menu
      onClick={e => this.handleExistsAtChange(e)}>
      <Menu.Item key='OM'>
        OM
      </Menu.Item>
      <Menu.Item key='SCM'>
        SCM
      </Menu.Item>
    </Menu>
  );

  handleExistsAtChange = (e: any) => {
    console.log("handleExistsAtChange", e.key);
    if (e.key === 'OM') {
      mismatchPrevKeyList = [0];
      this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, 0, 'SCM');
    }
    else {
      mismatchPrevKeyList = [0];
      this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, 0, 'OM');
    }
  };

  addfsoNonfsoKeyColumn = () => {
    // Inside the class component to access the React internal state
    const fsoNonfsoColumn = {
      title: <span>
        <Dropdown overlay={this.fsoNonfsoMenu} >
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

  fsoNonfsoMenu = () => (
    <Menu
      defaultSelectedKeys={['OM']}
      onClick={e => this.handlefsoNonfsoMenuChange(e)}>
      <Menu.Item key='fso'>
        FSO
      </Menu.Item>
      <Menu.Item key='nonFso'>
        Non FSO
      </Menu.Item>
    </Menu>
  );

  handlefsoNonfsoMenuChange = (e: any) => {
    if (e.key === 'fso') {
      openPrevKeyList = [""];
      this.fetchOpenKeys(true, false, this.state.DEFAULT_LIMIT, "");
    }
    else {
      openPrevKeyList = [""];
      this.fetchOpenKeys(false, true, this.state.DEFAULT_LIMIT, "");
    }
  };

  componentDidMount(): void {
    if (this.state.activeTab === '1') {
      this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.mismatchMissingState);
    } else if (this.state.activeTab === '2') {
      this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
    } else if (this.state.activeTab === '3') {
      keysPendingExpanded = [];
      this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
    } else if (this.state.activeTab === '4') {
      this.fetchDeletedKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
    }
  };

  componentWillUnmount(): void {
    cancelMismatchedEndpointSignal && cancelMismatchedEndpointSignal.abort();
    cancelOpenKeysSignal && cancelOpenKeysSignal.abort();
    cancelDeletePendingSignal && cancelDeletePendingSignal.abort();
    cancelDeletedKeysSignal && cancelDeletedKeysSignal.abort();
    cancelRowExpandSignal && cancelRowExpandSignal.abort();
  }

  fetchMismatchContainers = (limit: number, prevKeyMismatch: number, mismatchMissingState: any) => {
    this.setState({
      loading: true,
      nextClickable: true,
      prevClickable: true,
      mismatchMissingState
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal
    ]);

    const mismatchEndpoint = `/api/v1/containers/mismatch?limit=${limit}&prevKey=${prevKeyMismatch}&missingIn=${mismatchMissingState}`
    const { request, controller } = AxiosGetHelper(mismatchEndpoint, cancelMismatchedEndpointSignal)
    cancelMismatchedEndpointSignal = controller;
    request.then(mismatchContainersResponse => {
      const mismatchContainers: IContainerResponse[] = mismatchContainersResponse && mismatchContainersResponse.data && mismatchContainersResponse.data.containerDiscrepancyInfo;
      if (mismatchContainersResponse && mismatchContainersResponse.data && mismatchContainersResponse.data.lastKey === null) {
        //No Further Records may be last record
        mismatchPrevKeyList = [0];
        this.setState({
          loading: false,
          nextClickable: false,
          mismatchDataSource: mismatchContainers,
          expandedRowData: {},
        })
      }
      else {
        if (this.state.prevKeyMismatch === 0) {
          this.setState({
            prevClickable: false
          })
        }
        if (mismatchPrevKeyList.includes(mismatchContainersResponse.data.lastKey) === false) {
          mismatchPrevKeyList.push(mismatchContainersResponse.data.lastKey);
        }
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
      showDataFetchError(error.toString());
    });
  };

  fetchOpenKeys = (includeFso: boolean, includeNonFso: boolean, limit: number, prevKeyOpen: string) => {
    this.setState({
      loading: true,
      nextClickable: true,
      prevClickable: true,
      includeFso,
      includeNonFso
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal
    ]);

    let openKeysEndpoint;
    if (prevKeyOpen === "") {
      openKeysEndpoint = `/api/v1/keys/open?includeFso=${includeFso}&includeNonFso=${includeNonFso}&limit=${limit}`;
    }
    else {
      openKeysEndpoint = `/api/v1/keys/open?includeFso=${includeFso}&includeNonFso=${includeNonFso}&limit=${limit}&prevKey=${prevKeyOpen}`;
    }

    const { request, controller } = AxiosGetHelper(openKeysEndpoint, cancelOpenKeysSignal)
    cancelOpenKeysSignal = controller
    request.then(openKeysResponse => {
      const openKeys = openKeysResponse && openKeysResponse.data;
      let allopenKeysResponse: any[] = [];
      for (let key in openKeys) {
        if (Array.isArray(openKeys[key])) {
          openKeys[key] && openKeys[key].map((item: any) => (allopenKeysResponse.push({ ...item, type: key })));
        }
      }

      if (openKeysResponse && openKeysResponse.data && openKeysResponse.data.lastKey === "") {
        //last key of api is null may be last record no further records
        openPrevKeyList = [""];
        this.setState({
          loading: false,
          nextClickable: false,
          openKeysDataSource: allopenKeysResponse
        })
      }
      else {
        if (this.state.prevKeyOpen === "") {
          this.setState({
            prevClickable: false
          })
        }
        if (openPrevKeyList.includes(openKeysResponse.data.lastKey) === false) {
          openPrevKeyList.push(openKeysResponse.data.lastKey);
        }
        this.setState({
          loading: false,
          prevKeyOpen: openKeysResponse && openKeysResponse.data && openKeysResponse.data.lastKey,
          openKeysDataSource: allopenKeysResponse,
        })
      };
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });

  };

  fetchDeletePendingKeys = (limit: number, prevKeyDeletePending: string) => {
    this.setState({
      loading: true,
      nextClickable: true,
      prevClickable: true
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal
    ]);

    keysPendingExpanded = [];
    let deletePendingKeysEndpoint;
    if (prevKeyDeletePending === "" || prevKeyDeletePending === undefined) {
      deletePendingKeysEndpoint = `/api/v1/keys/deletePending?limit=${limit}`;
    }
    else {
      deletePendingKeysEndpoint = `/api/v1/keys/deletePending?limit=${limit}&prevKey=${prevKeyDeletePending}`;
    }

    const { request, controller } = AxiosGetHelper(deletePendingKeysEndpoint, cancelDeletePendingSignal);
    cancelDeletePendingSignal = controller;

    request.then(deletePendingKeysResponse => {
      const deletePendingKeys = deletePendingKeysResponse && deletePendingKeysResponse.data && deletePendingKeysResponse.data.deletedKeyInfo;
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
        }, { "dataSize": 0 });

        return {
          "dataSize": item.dataSize,
          "fileName": item.fileName,
          "keyName": item.keyName,
          "path": item.path,
          "keyCount": count
        }
      });

      if (deletePendingKeysResponse && deletePendingKeysResponse.data && deletePendingKeysResponse.data.lastKey === "") {
        //last key of api is empty may be last record no further records
        keysPendingPrevList = [""];
        this.setState({
          loading: false,
          nextClickable: false,
          pendingDeleteKeyDataSource: deletedKeyInfoData
        })
      }
      else {
        if (this.state.prevKeyDeletePending === "" || this.state.prevKeyDeletePending === undefined) {
          this.setState({
            prevClickable: false
          })
        }
        if (keysPendingPrevList.includes(deletePendingKeysResponse.data.lastKey) === false) {
          keysPendingPrevList.push(deletePendingKeysResponse.data.lastKey);
        }
        this.setState({
          loading: false,
          prevKeyDeletePending: deletePendingKeysResponse && deletePendingKeysResponse.data && deletePendingKeysResponse.data.lastKey,
          pendingDeleteKeyDataSource: deletedKeyInfoData
        });
      }
    }).catch(error => {
      this.setState({
        loading: false,
      });
      showDataFetchError(error.toString());
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

  fetchDeletedKeys = (limit: number, prevKeyDeleted: number) => {
    this.setState({
      loading: true,
      nextClickable: true,
      prevClickable: true
    });

    //Cancel any previous pending request
    cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal
    ]);

    const deletedKeysEndpoint = `/api/v1/containers/mismatch/deleted?limit=${limit}&prevKey=${prevKeyDeleted}`;
    const { request, controller } = AxiosGetHelper(deletedKeysEndpoint, cancelDeletedKeysSignal);
    cancelDeletedKeysSignal = controller
    request.then(deletedKeysResponse => {
      let deletedContainerKeys = [];
      deletedContainerKeys = deletedKeysResponse && deletedKeysResponse.data && deletedKeysResponse.data.containers;
      if (deletedKeysResponse && deletedKeysResponse.data && deletedKeysResponse.data.lastKey === null) {
        // no more further records last key
        deletedKeysPrevList = [0];
        this.setState({
          loading: false,
          nextClickable: false,
          deletedContainerKeysDataSource: deletedContainerKeys,
          expandedRowData: {},
        })
      }
      else {
        if (this.state.prevKeyDeleted === 0) {
          this.setState({
            prevClickable: false
          })
        }
        if (deletedKeysPrevList.includes(deletedKeysResponse.data.lastKey) === false) {
          deletedKeysPrevList.push(deletedKeysResponse.data.lastKey);
        }
        this.setState({
          loading: false,
          prevKeyDeleted: deletedKeysResponse && deletedKeysResponse.data && deletedKeysResponse.data.lastKey,
          deletedContainerKeysDataSource: deletedContainerKeys
        })
      };
    }).catch(error => {
      this.setState({
        loading: false
      });
      showDataFetchError(error.toString());
    });
  };

  changeTab = (activeKey: any) => {
    //when changing tab make empty all datasets and prevkey and deafult filtering to intial values also cancel all pending requests
    mismatchPrevKeyList = [0];
    openPrevKeyList = [""];
    keysPendingPrevList = [""];
    deletedKeysPrevList = [0];
    this.setState({
      activeTab: activeKey,
      mismatchDataSource: [],
      openKeysDataSource: [],
      pendingDeleteKeyDataSource: [],
      deletedContainerKeysDataSource: [],
      expandedRowData: {},
      prevKeyOpen: "",
      prevKeyDeletePending: "",
      prevKeyDeleted: 0,
      prevKeyMismatch: 0,
      mismatchMissingState: 'SCM',
      includeFso: true,
      includeNonFso: false,
      DEFAULT_LIMIT: 10,

    }, () => {
      if (activeKey === '2') {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
      } else if (activeKey === '3') {
        keysPendingExpanded = [];
        this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
      } else if (activeKey === '4') {
        this.fetchDeletedKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
      }
      else {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.mismatchMissingState);
      }
    })
  };

  fetchPreviousRecords = () => {
    // to fetch previous call stored all prevkey in array and fetching in respective tabs
    if (this.state.activeTab === '2') {
      this.setState({
        prevKeyOpen: openPrevKeyList[openPrevKeyList.indexOf(this.state.prevKeyOpen) - 2]
      }, () => {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
      })
    } else if (this.state.activeTab === '3') {
      this.setState({
        prevKeyDeletePending: keysPendingPrevList[keysPendingPrevList.indexOf(this.state.prevKeyDeletePending) - 2]
      }, () => {
        this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
      })
    } else if (this.state.activeTab === '4') {
      this.setState({
        prevKeyDeleted: deletedKeysPrevList[deletedKeysPrevList.indexOf(this.state.prevKeyDeleted) - 2]
      }, () => {
        this.fetchDeletedKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
      })
    }
    else {
      this.setState({
        prevKeyMismatch: mismatchPrevKeyList[mismatchPrevKeyList.indexOf(this.state.prevKeyMismatch) - 2]
      }, () => {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.mismatchMissingState);
      })
    }
  };

  fetchNextRecords = () => {
    // To Call API for Page Level for each page fetch next records
    if (this.state.activeTab === '2') {
      this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
    } else if (this.state.activeTab === '3') {
      this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
    } else if (this.state.activeTab === '4') {
      this.fetchDeletedKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
    }
    else {
      this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.mismatchMissingState);
    }
  };

  itemRender = (_: any, type: string, originalElement: any) => {
    if (type === 'prev') {
      return <div>{this.state.prevClickable ? <Link to="/Om" onClick={this.fetchPreviousRecords}> Prev</Link> : <Link to="/Om" style={{ pointerEvents: 'none' }}>No Records</Link>}</div>;
    }
    if (type === 'next') {
      return <div> {this.state.nextClickable ? <Link to="/Om" onClick={this.fetchNextRecords}> {'>>'} </Link> : <Link to="/Om" style={{ pointerEvents: 'none' }}>No More Further Records</Link>}</div>;
    }
    return originalElement;
  };

  onShowSizeChange = (current: number, pageSize: number) => {
    if (this.state.activeTab === '2') {
      //open keys
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyOpen: openPrevKeyList[openPrevKeyList.indexOf(this.state.prevKeyOpen) - 1]
      }, () => {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
      });
    }
    else if (this.state.activeTab === '3') {
      //keys pending for deletion
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyDeletePending: keysPendingPrevList[keysPendingPrevList.indexOf(this.state.prevKeyDeletePending) - 1]
      }, () => {
        this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
      })
    }
    else if (this.state.activeTab === '4') {
      //deleted container keys
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyDeleted: deletedKeysPrevList[deletedKeysPrevList.indexOf(this.state.prevKeyDeleted) - 1]
      }, () => {
        this.fetchDeletedKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
      })
    }
    else {
      // active tab 1 for mismatch
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyMismatch: mismatchPrevKeyList[mismatchPrevKeyList.indexOf(this.state.prevKeyMismatch) - 1]
      }, () => {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.mismatchMissingState);
      });
    }
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
        if (error.name === "CanceledError") {
          showDataFetchError(cancelRowExpandSignal.signal.reason)
        }
        else {
          console.log(error);
          showDataFetchError(error.toString());
        }
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

  render() {
    const { mismatchDataSource, loading, openKeysDataSource, pendingDeleteKeyDataSource, deletedContainerKeysDataSource } = this.state;

    const paginationConfig: TablePaginationConfig = {
      pageSize: this.state.DEFAULT_LIMIT,
      defaultPageSize: this.state.DEFAULT_LIMIT,
      pageSizeOptions: ['10', '20', '30', '50'],
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange,
      itemRender: this.itemRender
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
        locale={{ filterTitle: '' }} />
    }

    const generateOpenKeyTable = (dataSource: any) => {
      return <Table
        expandable={{
          expandRowByClick: true
        }} dataSource={dataSource}
        columns={this.searchOpenKeyColumn()}
        loading={loading} rowKey='path'
        pagination={paginationConfig}
        locale={{ filterTitle: '' }} />
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
        pagination={paginationConfig} rowKey='keyName' />
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
        locale={{ filterTitle: '' }} />
    }



    return (
      <div className='missing-containers-container'>
        <div className='page-header'>
          OM DB Insights
        </div>
        <div className='content-div'>
          <Tabs defaultActiveKey={this.state.activeTab} onChange={this.changeTab}>
            <Tabs.TabPane key='1' tab={`Container Mismatch Info`}>
              {generateMismatchTable(mismatchDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane key='2' tab={`Open Keys`}>
              {generateOpenKeyTable(openKeysDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane key='3'
              tab={<label>Keys Pending for Deletion&nbsp;&nbsp;
                <Tooltip placement='top' title="Keys that are pending for deletion.">
                  <InfoCircleOutlined />
                </Tooltip>
              </label>
              }>
              {generateKeysPendingTable(pendingDeleteKeyDataSource)}
            </Tabs.TabPane>
            <Tabs.TabPane key='4'
              tab={<label>Deleted Container Keys&nbsp;&nbsp;
                <Tooltip placement='top' title={"Keys mapped to Containers in DELETED state SCM."}>
                  <InfoCircleOutlined />
                </Tooltip>
              </label>
              }>
              {generateDeletedKeysTable(deletedContainerKeysDataSource)}
            </Tabs.TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}