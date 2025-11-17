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
import { Table, Tag } from 'antd';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  CloudServerOutlined,
  FileUnknownOutlined,
  HddOutlined,
  LaptopOutlined,
  SaveOutlined
} from '@ant-design/icons';
import {
  ColumnProps,
  TablePaginationConfig
} from 'antd/es/table';
import { ActionMeta, ValueType } from 'react-select';
import CreatableSelect from "react-select/creatable";

import {
  BucketLayout,
  BucketLayoutTypeList,
  BucketStorage,
  BucketStorageTypeList,
  IAcl,
  IBucket
} from '@/types/om.types';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import { MultiSelect, IOption } from '@/components/multiSelect/multiSelect';
import { AclPanel } from '@/components/aclDrawer/aclDrawer';
import { ColumnSearch } from '@/utils/columnSearch';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { AxiosGetHelper } from "@/utils/axiosRequestHelper";
import { nullAwareLocaleCompare, showDataFetchError } from '@/utils/common';
import QuotaBar from '@/components/quotaBar/quotaBar';

import './buckets.less';


interface IBucketResponse {
  volumeName: string;
  name: string;
  versioning: boolean;
  storageType: string;
  bucketLayout: string;
  creationTime: number;
  modificationTime: number;
  sourceVolume: string;
  sourceBucket: string;
  usedBytes: number;
  usedNamespace: number;
  quotaInBytes: number;
  quotaInNamespace: number;
  owner: string;
  acls?: IAcl[];
}

interface IBucketsResponse {
  totalCount: number;
  buckets: IBucketResponse[];
}

type BucketTableColumn = ColumnProps<any> & any;

interface IBucketsState {
  loading: boolean;
  totalCount: number;
  lastUpdated: number;
  selectedColumns: IOption[];
  columnOptions: IOption[];
  volumeBucketMap: Map<string, Set<IBucket>>;
  selectedVolumes: IOption[];
  selectedBuckets: IBucket[];
  volumeOptions: IOption[];
  currentRow?: IBucket;
  showPanel: boolean;
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

const renderIsVersionEnabled = (isVersionEnabled: boolean) => {
  return isVersionEnabled ?
    <CheckCircleOutlined style={{ color: '#1da57a' }} className='icon-success' /> :
    <CloseCircleOutlined className='icon-neutral' />
};

const renderStorageType = (bucketStorage: BucketStorage) => {
  const bucketStorageIconMap = {
    RAM_DISK: <LaptopOutlined />,
    SSD: <SaveOutlined />,
    DISK: <HddOutlined />,
    ARCHIVE: <CloudServerOutlined />
  };
  const icon = bucketStorage in bucketStorageIconMap
    ? bucketStorageIconMap[bucketStorage]
    : <FileUnknownOutlined />;
  return <span>{icon} {bucketStorage}</span>;
};

const renderBucketLayout = (bucketLayout: BucketLayout) => {
  const bucketLayoutColorMap = {
    FILE_SYSTEM_OPTIMIZED: 'green',
    OBJECT_STORE: 'blue',
    LEGACY: 'gray'
  };
  const color = bucketLayout in bucketLayoutColorMap ?
    bucketLayoutColorMap[bucketLayout] : '';
  return <Tag color={color}>{bucketLayout}</Tag>;
};

const COLUMNS: BucketTableColumn[] = [
  {
    title: 'Bucket',
    dataIndex: 'bucketName',
    key: 'bucketName',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IBucket, b: IBucket) => a.bucketName.localeCompare(b.bucketName),
    defaultSortOrder: 'ascend' as const,
    fixed: 'left'
  },
  {
    title: 'Volume',
    dataIndex: 'volumeName',
    key: 'volumeName',
    isVisible: true,
    isSearchable: true,
    sorter: (a: IBucket, b: IBucket) => a.volumeName.localeCompare(b.volumeName),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Owner',
    dataIndex: 'owner',
    key: 'owner',
    isVisible: true,
    sorter: (a: IBucket, b: IBucket) => nullAwareLocaleCompare(a.owner, b.owner)
  },
  {
    title: 'Versioning',
    dataIndex: 'isVersionEnabled',
    isVisible: true,
    key: 'isVersionEnabled',
    render: (isVersionEnabled: boolean) => renderIsVersionEnabled(isVersionEnabled)
  },
  {
    title: 'Storage Type',
    dataIndex: 'storageType',
    key: 'storageType',
    isVisible: true,
    filterMultiple: true,
    filters: BucketStorageTypeList.map(state => ({ text: state, value: state })),
    onFilter: (value: BucketStorage, record: IBucket) => record.storageType === value,
    sorter: (a: IBucket, b: IBucket) => a.storageType.localeCompare(b.storageType),
    render: (storageType: BucketStorage) => renderStorageType(storageType)
  },
  {
    title: 'Bucket Layout',
    dataIndex: 'bucketLayout',
    key: 'bucketLayout',
    isVisible: true,
    filterMultiple: true,
    filters: BucketLayoutTypeList.map(state => ({ text: state, value: state })),
    onFilter: (value: BucketLayout, record: IBucket) => record.bucketLayout === value,
    sorter: (a: IBucket, b: IBucket) => a.bucketLayout.localeCompare(b.bucketLayout),
    render: (bucketLayout: BucketLayout) => renderBucketLayout(bucketLayout)
  },
  {
    title: 'Creation Time',
    dataIndex: 'creationTime',
    key: 'creationTime',
    isVisible: true,
    sorter: (a: IBucket, b: IBucket) => a.creationTime - b.creationTime,
    render: (creationTime: number) => {
      return creationTime > 0 ? moment(creationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Modification Time',
    dataIndex: 'modificationTime',
    key: 'modificationTime',
    isVisible: true,
    sorter: (a: IBucket, b: IBucket) => a.modificationTime - b.modificationTime,
    render: (modificationTime: number) => {
      return modificationTime > 0 ? moment(modificationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Storage Capacity',
    key: 'quotaCapacityBytes',
    isVisible: true,
    sorter: (a: IBucket, b: IBucket) => a.usedBytes - b.usedBytes,
    render: (text: string, record: IBucket) => (
      <QuotaBar
        quota={record.quotaInBytes}
        used={record.usedBytes}
        quotaType='size'
      />
    )
  },
  {
    title: 'Namespace Capacity',
    key: 'namespaceCapacity',
    isVisible: true,
    sorter: (a: IBucket, b: IBucket) => a.usedNamespace - b.usedNamespace,
    render: (text: string, record: IBucket) => (
      <QuotaBar
        quota={record.quotaInNamespace}
        used={record.usedNamespace}
        quotaType='namespace'
      />
    )
  },
  {
    title: 'Source Volume',
    dataIndex: 'sourceVolume',
    key: 'sourceVolume',
    render: (sourceVolume: string) => {
      return sourceVolume ? sourceVolume : 'NA';
    }
  },
  {
    title: 'Source Bucket',
    dataIndex: 'sourceBucket',
    key: 'sourceBucket',
    render: (sourceBucket: string) => {
      return sourceBucket ? sourceBucket : 'NA';
    }
  }
];

const allColumnsOption: IOption = {
  label: 'Select all',
  value: '*'
};

const allVolumesOption: IOption = {
  label: 'All Volumes',
  value: '*'
};

const defaultColumns: IOption[] = COLUMNS.map(column => ({
  label: column.key,
  value: column.key
}));

let cancelSignal: AbortController;

export class Buckets extends React.Component<Record<string, object>, IBucketsState> {
  autoReload: AutoReloadHelper;

  constructor(props = {}) {
    super(props);
    this._addAclColumn();
    this.state = {
      loading: false,
      totalCount: 0,
      lastUpdated: 0,
      selectedColumns: [],
      columnOptions: defaultColumns,
      volumeBucketMap: new Map<string, Set<IBucket>>(),
      selectedBuckets: [],
      selectedVolumes: [],
      volumeOptions: [],
      showPanel: false,
      currentRow: {},
      selectedLimit: INITIAL_LIMIT_OPTION
    };
    this.autoReload = new AutoReloadHelper(this._loadData);
  }

  _addAclColumn = () => {
    // Inside the class component to access the React internal state
    const aclLinkColumn: BucketTableColumn = {
      title: 'ACLs',
      dataIndex: 'acls',
      key: 'acls',
      isVisible: true,
      render: (_: any, record: IBucket) => {
        return (
          <a
            key='acl'
            onClick={() => {
              this._handleAclLinkClick(record);
            }}
          >
            Show ACL
          </a>
        );
      }
    };

    if (COLUMNS.length > 0 && COLUMNS[COLUMNS.length - 1].key !== 'acls') {
      // Push the ACL column for initial
      COLUMNS.push(aclLinkColumn);
    } else {
      // Replace old ACL column with new ACL column with correct reference
      // e.g. After page is reloaded / redirect from other page
      COLUMNS[COLUMNS.length - 1] = aclLinkColumn;
    }

    if (defaultColumns.length > 0 && defaultColumns[defaultColumns.length - 1].label !== 'acls') {
      defaultColumns.push({
        label: aclLinkColumn.key,
        value: aclLinkColumn.key
      });
    }
  };

  _handleColumnChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const selectedColumns = (selected as IOption[]);
    this.setState({
      selectedColumns,
      showPanel: false
    });
  };

  _handleLimitChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const selectedLimit = (selected as IOption)
    this.setState({
      selectedLimit
    }, this._loadData)
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

  _handleVolumeChange = (selected: ValueType<IOption>, _action: ActionMeta<IOption>) => {
    const { volumeBucketMap } = this.state;
    const selectedVolumes = (selected as IOption[]);

    let selectedBuckets: IBucket[] = [];

    if (selectedVolumes && selectedVolumes.length > 0) {
      selectedVolumes.forEach(selectedVolume => {
        if (volumeBucketMap.has(selectedVolume.value) && volumeBucketMap.get(selectedVolume.value)) {
          const bucketsUnderVolume: IBucket[] = Array.from(volumeBucketMap.get(selectedVolume.value)!);
          selectedBuckets = [...selectedBuckets, ...bucketsUnderVolume];
        }
      });
    }

    this.setState({
      selectedVolumes,
      selectedBuckets
    });
  };

  _getSelectedColumns = (selected: IOption[]) => {
    const selectedColumns = selected.length > 0 ? selected : COLUMNS.filter(column => column.isVisible).map(column => ({
      label: column.key,
      value: column.key
    }));
    return selectedColumns;
  };

  _handleAclLinkClick = (bucket: IBucket) => {
    this.setState({
      showPanel: true,
      currentRow: bucket
    });
  };

  _getVolumeSearchParam = () => {
    const searchParams = new URLSearchParams(this.props.location.search as string);
    return searchParams.get('volume');
  };

  _loadData = () => {
    this.setState(prevState => ({
      loading: true,
      selectedColumns: this._getSelectedColumns(prevState.selectedColumns),
      showPanel: false
    }));

    const { request, controller } = AxiosGetHelper(
      '/api/v1/buckets',
      cancelSignal,
      '',
      { limit: this.state.selectedLimit.value }
    );
    cancelSignal = controller;
    request.then(response => {
      const bucketsResponse: IBucketsResponse = response.data;
      const totalCount = bucketsResponse.totalCount;
      const buckets: IBucketResponse[] = bucketsResponse.buckets;

      const dataSource: IBucket[] = buckets.map(bucket => {
        return {
          volumeName: bucket.volumeName,
          bucketName: bucket.name,
          isVersionEnabled: bucket.versioning,
          storageType: bucket.storageType,
          bucketLayout: bucket.bucketLayout,
          creationTime: bucket.creationTime,
          modificationTime: bucket.modificationTime,
          sourceVolume: bucket.sourceVolume,
          sourceBucket: bucket.sourceBucket,
          usedBytes: bucket.usedBytes,
          usedNamespace: bucket.usedNamespace,
          quotaInBytes: bucket.quotaInBytes,
          quotaInNamespace: bucket.quotaInNamespace,
          owner: bucket.owner,
          acls: bucket.acls
        };
      });

      // Map for fast buckets lookup based on volume
      // Act as the base data source
      const volumeBucketMap: Map<string, Set<IBucket>> = dataSource.reduce(
        (map: Map<string, Set<IBucket>>, current) => {
          const volume = current.volumeName;
          if (map.has(volume)) {
            const buckets = Array.from(map.get(volume)!);
            map.set(volume, new Set([...buckets, current]));
          } else {
            map.set(volume, new Set().add(current));
          }

          return map;
        }, new Map<string, Set<IBucket>>());

      // Set options for volume selection dropdown
      const volumeOptions: IOption[] = Array.from(volumeBucketMap.keys()).map(k => ({
        label: k,
        value: k
      }));

      this.setState({
        loading: false,
        totalCount,
        volumeBucketMap,
        volumeOptions,
        lastUpdated: Number(moment()),
        showPanel: false
      }, () => {
        if (!this.state.selectedVolumes || this.state.selectedVolumes.length === 0) {
          // Select all volumes if it is first page load and volume search param is not specified
          this._handleVolumeChange([allVolumesOption, ...volumeOptions], { action: 'select-option' });
        } else {
          // The selected volumes remain unchanged if volumes have been previously selected
          this._handleVolumeChange(this.state.selectedVolumes, { action: 'select-option' });
        }
      });
    }).catch(error => {
      this.setState({
        loading: false,
        showPanel: false
      });
      showDataFetchError(error);
    });
  };

  componentDidMount(): void {
    // For initial page (re)load, we get the volume from the URL search param
    const initialVolume = this._getVolumeSearchParam();
    if (initialVolume) {
      const initialVolumeOption = { label: initialVolume, value: initialVolume };
      this.setState({
        selectedVolumes: [initialVolumeOption]
      });
    }

    // Fetch buckets on component mount
    this._loadData();
    this.autoReload.startPolling();
  }

  componentWillUnmount(): void {
    this.autoReload.stopPolling();
    cancelSignal && cancelSignal.abort();
  }

  onShowSizeChange = (current: number, pageSize: number) => {
    console.log(current, pageSize);
  };

  render() {
    const { loading, totalCount, lastUpdated, selectedColumns,
      columnOptions, volumeOptions, selectedVolumes,
      selectedBuckets, showPanel, currentRow, selectedLimit } = this.state;
    const paginationConfig: TablePaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} buckets`,
      showSizeChanger: true,
      onShowSizeChange: this.onShowSizeChange
    };
    return (
      <div className='buckets-container'>
        <div className='page-header'>
          Buckets ({totalCount})
          <div className='filter-block'>
            <MultiSelect
              allowSelectAll
              isMulti
              className='multi-select-container'
              options={volumeOptions}
              closeMenuOnSelect={false}
              hideSelectedOptions={false}
              value={selectedVolumes}
              allOption={allVolumesOption}
              onChange={this._handleVolumeChange}
            />
            Volumes
          </div>
          <div className='filter-block'>
            <MultiSelect
              allowSelectAll
              isMulti
              maxShowValues={3}
              className='multi-select-container'
              options={columnOptions}
              closeMenuOnSelect={false}
              hideSelectedOptions={false}
              value={selectedColumns}
              allOption={allColumnsOption}
              isOptionDisabled={(option) => option.value === "bucketName"}
              onChange={this._handleColumnChange}
            /> Columns
          </div>
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
          <AutoReloadPanel
            isLoading={loading}
            lastRefreshed={lastUpdated}
            togglePolling={this.autoReload.handleAutoReloadToggle}
            onReload={this._loadData}
          />
        </div>

        <div className='content-div'>
          <Table
            dataSource={selectedBuckets}
            columns={COLUMNS.reduce<any[]>((filtered, column) => {
              if (selectedColumns && selectedColumns.some(e => e.value === column.key)) {
                if (column.isSearchable) {
                  const newColumn = {
                    ...column,
                    ...new ColumnSearch(column).getColumnSearchProps(column.dataIndex)
                  };
                  filtered.push(newColumn);
                } else {
                  filtered.push(column);
                }
              }

              return filtered;
            }, [])}
            loading={loading}
            pagination={paginationConfig}
            rowKey={(bucketRecord: IBucket) => `${bucketRecord.volumeName}_${bucketRecord.bucketName}`}
            scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
            locale={{ filterTitle: '' }}
          />
        </div>
        <AclPanel visible={showPanel} acls={currentRow.acls} objName={currentRow.bucketName} objType='Bucket' />
      </div>
    );
  }
}
