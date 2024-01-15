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
import { Table, Tabs, Menu, Dropdown, Icon, Tooltip } from 'antd';
import { PaginationConfig } from 'antd/lib/pagination';
import filesize from 'filesize';
import moment from 'moment';
import { showDataFetchError, byteToSize } from 'utils/common';
import './om.less';
import { ColumnSearch } from 'utils/columnSearch';
import { Link } from 'react-router-dom';
import { AxiosGetHelper, cancelRequests } from 'utils/axiosRequestHelper';


const size = filesize.partial({ standard: 'iec' });
const { TabPane } = Tabs;
//Previous Key Need to store respective Lastkey of each API

let keysPendingExpanded: any = [];
const prevKeyListMap = new Map();

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
    title: 'Key Name',
    dataIndex: 'path',
    key: 'path',
    isSearchable: true
  },
  {
    title: 'Amount of data',
    dataIndex: 'size',
    key: 'size',
    render: (size :any) => size = byteToSize(size,1)
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
    render: (dataSize :any) => dataSize = byteToSize(dataSize,1)
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
        {pipelines && pipelines.map((pipeline:any) => (
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
  prevClickable: boolean;
  pageDisplayCount: number;
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
      prevClickable: false,
      pageDisplayCount: 1
    };
  }

  addexistAtColumn = () => {
    // Inside the class component to access the React internal state
    const existsAtColumn = {
      title: <span>
        <Dropdown overlay={this.existAtScmOmMenu} >
            <label> Exists at&nbsp;&nbsp;
              <Icon type="funnel-plot" theme="filled" />&nbsp;&nbsp;&nbsp;&nbsp;
            </label>
        </Dropdown>&nbsp;&nbsp;
        <label>
          <Tooltip placement='top' title={<span>{'SCM: Container exist at SCM but missing at OM.'}<br />
            {'OM: Container exist at OM but missing at SCM.'}</span>}>
          <Icon type='info-circle' />
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
    this.setState({
      pageDisplayCount: 1
    }, () => {
      if (e.key === 'OM') {
        //mismatchPrevKeyList = [0];
        prevKeyListMap.clear();
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, 0, 'SCM');
      }
      else {
        //mismatchPrevKeyList = [0];
        prevKeyListMap.clear();
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, 0, 'OM');
      }
    })
  };

  addfsoNonfsoKeyColumn = () => {
    // Inside the class component to access the React internal state
    const fsoNonfsoColumn = {
      title: <span>
        <Dropdown overlay={this.fsoNonfsoMenu} >
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
    console.log("Non FSO handle", e.key);
    this.setState({
      pageDisplayCount: 1
    }, () => {
      if (e.key === 'fso') {
        prevKeyListMap.clear();
        this.fetchOpenKeys(true, false, this.state.DEFAULT_LIMIT, "");
      }
      else {
        prevKeyListMap.clear();
        this.fetchOpenKeys(false, true, this.state.DEFAULT_LIMIT, "");
      }
    })
  };

  componentDidMount(): void {
    if (this.state.activeTab  === '1') {
      this.fetchMismatchContainers(this.state.DEFAULT_LIMIT, this.state.prevKeyMismatch, this.state.mismatchMissingState);
    } else if (this.state.activeTab === '2') {
      this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
    } else if (this.state.activeTab  === '3') {
      keysPendingExpanded =[];
      this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
    } else if (this.state.activeTab  === '4') {
      this.fetchDeletedKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
    }
  };

  componentWillUnmount(): void {
    cancelMismatchedEndpointSignal && cancelMismatchedEndpointSignal.abort();
    cancelOpenKeysSignal && cancelOpenKeysSignal.abort();
    cancelDeletePendingSignal && cancelDeletePendingSignal.abort();
    cancelDeletedKeysSignal && cancelDeletedKeysSignal.abort();
    cancelRowExpandSignal && cancelRowExpandSignal.abort();
    prevKeyListMap.clear();
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
      if (mismatchContainers && mismatchContainers.length === 0) {
        //No Further Records may be last record
        this.setState({
          loading: false,
          nextClickable: false,
          mismatchDataSource: mismatchContainers,
          expandedRowData: {},
        })
      }
      else {
        if (this.state.prevKeyMismatch === 0 || this.state.pageDisplayCount === 1){
          this.setState({
            prevClickable: false
          })
        }
        //Need to avoid rewrite in Map so avoiding duplication and wrong value for
        if (!prevKeyListMap.has(this.state.pageDisplayCount)) {
          prevKeyListMap.set(this.state.pageDisplayCount, prevKeyMismatch);
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

      if (allopenKeysResponse && allopenKeysResponse.length === 0) {
        //last key of api is null may be last record no further records
        this.setState({
          loading: false,
          nextClickable: false,
          openKeysDataSource: allopenKeysResponse
        })
      }
      else {
         if (this.state.prevKeyOpen === "" || this.state.pageDisplayCount === 1){
          this.setState({
            prevClickable: false
          })
        }
         // To avoid Duplicates values to rewrite into Map
        if (!prevKeyListMap.has(this.state.pageDisplayCount)) {
          prevKeyListMap.set(this.state.pageDisplayCount, prevKeyOpen);
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
      prevClickable :true
    });

     //Cancel any previous pending request
     cancelRequests([
      cancelMismatchedEndpointSignal,
      cancelOpenKeysSignal,
      cancelDeletePendingSignal,
      cancelDeletedKeysSignal,
      cancelRowExpandSignal
    ]);

    keysPendingExpanded =[];
    let deletePendingKeysEndpoint;
    if (prevKeyDeletePending === "" || prevKeyDeletePending === undefined ) {
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
      deletedKeyInfoData = deletePendingKeys && deletePendingKeys.flatMap((infoObject:any) => {
        const { omKeyInfoList } = infoObject;
        keysPendingExpanded.push(infoObject);
        let count = 0;
        let item = omKeyInfoList && omKeyInfoList.reduce((obj:any, item:any) => {
          const { dataSize } = item;
          const newDataSize = obj.dataSize + dataSize;
          count = count + 1;
          return { ...item, dataSize: newDataSize };
        }, { "dataSize": 0 });
      
        return {
          "dataSize": item.dataSize,
          "fileName":item.fileName,
          "keyName": item.keyName,
          "path": item.path,
          "keyCount": count
        }
      });

      if ( deletedKeyInfoData === undefined || deletedKeyInfoData.length === 0) {
        //last key of api is empty may be last record no further records
        if (prevKeyListMap.has(this.state.pageDisplayCount)) {
          // Getting empty result  because keys are present for Fractions of seconds will disapper after some time
          this.setState({
            loading: false,
            nextClickable: false,
            pendingDeleteKeyDataSource: deletedKeyInfoData,
            prevClickable: false
          })
        }
        else {
          this.setState({
            loading: false,
            nextClickable: false,
            pendingDeleteKeyDataSource: deletedKeyInfoData
          })
        }
      }
      else {
           if (this.state.prevKeyDeletePending === "" || this.state.prevKeyDeletePending === undefined || this.state.pageDisplayCount === 1 ){
          this.setState({
            prevClickable: false
          })
        }
         //Map Key Set to avoid rewrite
        if (!prevKeyListMap.has(this.state.pageDisplayCount)) {
          prevKeyListMap.set(this.state.pageDisplayCount, prevKeyDeletePending);
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

  expandedKey =  ( record:any)=> {
    const filteredData = keysPendingExpanded && keysPendingExpanded.flatMap((info:any) =>
    info.omKeyInfoList && info.omKeyInfoList.filter((item: any) => item.keyName === record.keyName)
    )
    const columns= [{
      title: 'Data Size',
      dataIndex: 'dataSize',
      key: 'dataSize',
      render: (dataSize :any) => dataSize = dataSize > 0 ? byteToSize(dataSize,1) : dataSize
    },
    {
      title: 'Replicated Data Size',
      dataIndex: 'replicatedSize',
      key: 'replicatedSize',
      render: (replicatedSize :any) => replicatedSize = replicatedSize > 0 ? byteToSize(replicatedSize,1) : replicatedSize
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
      locale={{filterTitle: ""}}
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
      if (deletedContainerKeys === undefined || deletedContainerKeys.length === 0) {
        // no more further records last key
        this.setState({
          loading: false,
          nextClickable: false,
          deletedContainerKeysDataSource: deletedContainerKeys,
          expandedRowData: {},
        })
      }
      else {
        if (this.state.prevKeyDeleted === 0 || this.state.pageDisplayCount === 1 ){
          this.setState({
            prevClickable: false
          })
        }
        if (!prevKeyListMap.has(this.state.pageDisplayCount)) {
          prevKeyListMap.set(this.state.pageDisplayCount, prevKeyDeleted);
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
    prevKeyListMap.clear();
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
      pageDisplayCount: 1

    }, () => {
      if (activeKey === '2') {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT, this.state.prevKeyOpen);
      } else if (activeKey === '3') {
        keysPendingExpanded =[];
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
          prevKeyOpen: prevKeyListMap.get(this.state.pageDisplayCount - 1),
          pageDisplayCount : this.state.pageDisplayCount - 1
        }, () => {
          this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT,this.state.prevKeyOpen);
        })
    } else if (this.state.activeTab === '3') {
      this.setState({
        prevKeyDeletePending: prevKeyListMap.get(this.state.pageDisplayCount - 1),
        pageDisplayCount: this.state.pageDisplayCount - 1
      }, () => {
        this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
      })
    } else if (this.state.activeTab === '4') {
      this.setState({
        prevKeyDeleted: prevKeyListMap.get(this.state.pageDisplayCount - 1),
        pageDisplayCount : this.state.pageDisplayCount- 1
      }, () => {
        this.fetchDeletedKeys(this.state.DEFAULT_LIMIT,this.state.prevKeyDeleted);
      })
    }
      else {
        this.setState({
          prevKeyMismatch: prevKeyListMap.get(this.state.pageDisplayCount -1),
          pageDisplayCount : this.state.pageDisplayCount- 1
        }, () => {
          this.fetchMismatchContainers(this.state.DEFAULT_LIMIT,this.state.prevKeyMismatch, this.state.mismatchMissingState);
        })
      }
  };

  fetchNextRecords = () => {
    // To Call API for Page Level for each page fetch next records
    // First Increment Count and then call respective API
    this.setState({
      pageDisplayCount : this.state.pageDisplayCount + 1
    }, () => {
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
  })
  };

  itemRender = (_: any, type: string, originalElement: any) => {
    if (type === 'prev') {
      return <>{this.state.prevClickable && this.state.pageDisplayCount && <Link to="/Om" className='ant-pagination-item-link' onClick={this.fetchPreviousRecords}> {'<'}
      </Link>}</>;
    }
    if (type === 'page') {
      return <>{this.state.pageDisplayCount}</>
    }
    if (type === 'next') {
      return <>{this.state.nextClickable ? <Link to="/Om" className='ant-pagination-item-link next' onClick={this.fetchNextRecords}> {'>'} </Link> : <div className='norecords'>No Records</div> }</>;
    }
    return originalElement;
  };

  onShowSizeChange = (current: number, pageSize: number) => {
    if (this.state.activeTab === '2') {
      //open keys
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyOpen: prevKeyListMap.get(this.state.pageDisplayCount)
      }, () => {
        this.fetchOpenKeys(this.state.includeFso, this.state.includeNonFso, this.state.DEFAULT_LIMIT,this.state.prevKeyOpen);
      });
    }
    else if (this.state.activeTab === '3') {
      //keys pending for deletion
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyDeletePending: prevKeyListMap.get(this.state.pageDisplayCount)
      }, () => {
        this.fetchDeletePendingKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeletePending);
      })
    }
    else if (this.state.activeTab === '4') {
      //deleted container keys
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyDeleted: prevKeyListMap.get(this.state.pageDisplayCount)
      }, () => {
        this.fetchDeletedKeys(this.state.DEFAULT_LIMIT, this.state.prevKeyDeleted);
      })
    }
    else {
      // active tab 1 for mismatch
      this.setState({
        DEFAULT_LIMIT: pageSize,
        prevKeyMismatch: prevKeyListMap.get(this.state.pageDisplayCount)
      }, () => {
        this.fetchMismatchContainers(this.state.DEFAULT_LIMIT,this.state.prevKeyMismatch, this.state.mismatchMissingState);
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
      const paginationConfig: PaginationConfig = {
        showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} keys`
      };
      return (
        <Table
          loading={containerKeys.loading} dataSource={dataSource}
          columns={KEY_TABLE_COLUMNS} pagination={paginationConfig}
          rowKey='uid'
          locale={{filterTitle: ""}}/>
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

    const paginationConfig: PaginationConfig = {
      pageSize:this.state.DEFAULT_LIMIT,
      defaultPageSize: this.state.DEFAULT_LIMIT,
      pageSizeOptions: ['10', '30', '50', '100'],
      showSizeChanger: this.state.nextClickable ? true : false,
      onShowSizeChange: this.onShowSizeChange,
      itemRender: this.itemRender,
      total: this.state.pageDisplayCount - 1
    };

    const generateMismatchTable = (dataSource: any) => {
      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchMismatchColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='containerId'
        expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}
        locale={{filterTitle: ""}}/>
    }

    const generateOpenKeyTable = (dataSource: any) => {
      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchOpenKeyColumn()}
        loading={loading} rowKey='key'
        pagination={paginationConfig}
        locale={{filterTitle: ""}} />
    }

    const generateKeysPendingTable = (dataSource: any) => {
      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchKeysPendingColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='keyName'
        expandedRowRender={this.expandedKey} />
    }

    const generateDeletedKeysTable = (dataSource: any) => {
      return <Table
        expandRowByClick dataSource={dataSource}
        columns={this.searchDeletedKeyColumn()}
        loading={loading}
        pagination={paginationConfig} rowKey='containerId'
        expandedRowRender={this.expandedRowRender} onExpand={this.onRowExpandClick}
        locale={{filterTitle: ""}}
      />
    }

  

    return (
      <div className='missing-containers-container'>
        <div className='page-header'>
          OM DB Insights
        </div>
        <div className='content-div'>
          <Tabs defaultActiveKey={this.state.activeTab} onChange={this.changeTab}>
            <TabPane key='1' tab={`Container Mismatch Info`}>
              {generateMismatchTable(mismatchDataSource)}
            </TabPane>
            <TabPane key='2' tab={`Open Keys`}>
              {generateOpenKeyTable(openKeysDataSource)}
            </TabPane>
            <TabPane key='3'
              tab={<label>Keys Pending for Deletion&nbsp;&nbsp;
                <Tooltip placement='top' title="Keys that are pending for deletion.">
                  <Icon type='info-circle' />
                </Tooltip>
              </label>
              }>
              {generateKeysPendingTable(pendingDeleteKeyDataSource)}
            </TabPane>
            <TabPane key='4'
              tab={<label>Deleted Container Keys&nbsp;&nbsp;
                <Tooltip placement='top' title={"Keys mapped to Containers in DELETED state SCM."}>
                  <Icon type='info-circle' />
                </Tooltip>
              </label>
              }>
              {generateDeletedKeysTable(deletedContainerKeysDataSource)}
            </TabPane>
          </Tabs>
        </div>
      </div>
    );
  }
}