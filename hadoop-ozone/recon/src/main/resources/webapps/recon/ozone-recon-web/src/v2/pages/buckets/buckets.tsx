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

import React, { useCallback, useEffect, useState } from 'react';
import moment from 'moment';
import { Table, Tag } from 'antd';
import {
  ColumnProps,
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  CloudServerOutlined,
  FileUnknownOutlined,
  HddOutlined,
  LaptopOutlined,
  SaveOutlined
} from '@ant-design/icons';
import { ValueType } from 'react-select';
import { useLocation } from 'react-router-dom';

import QuotaBar from '@/components/quotaBar/quotaBar';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import AclPanel from '@/v2/components/aclDrawer/aclDrawer';
import Search from '@/v2/components/search/search';
import MultiSelect from '@/v2/components/select/multiSelect';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';

import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { AxiosGetHelper } from "@/utils/axiosRequestHelper";
import { nullAwareLocaleCompare, showDataFetchError } from '@/utils/common';
import { useDebounce } from '@/v2/hooks/debounce.hook';

import {
  Bucket,
  BucketLayout,
  BucketLayoutTypeList,
  BucketResponse,
  BucketsState,
  BucketStorage,
  BucketStorageTypeList
} from '@/v2/types/bucket.types';

import './buckets.less';


const LIMIT_OPTIONS: Option[] = [
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

const renderIsVersionEnabled = (isVersionEnabled: boolean) => {
  return isVersionEnabled
    ? <CheckCircleOutlined
      style={{ color: '#1da57a' }}
      className='icon-success' />
    : <CloseCircleOutlined className='icon-neutral' />
};

const renderStorageType = (bucketStorage: BucketStorage) => {
  const bucketStorageIconMap: Record<BucketStorage, React.ReactElement> = {
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

const SearchableColumnOpts = [{
  label: 'Bucket',
  value: 'name'
}, {
  label: 'Volume',
  value: 'volumeName'
}]

const COLUMNS: ColumnsType<Bucket> = [
  {
    title: 'Bucket',
    dataIndex: 'name',
    key: 'name',
    sorter: (a: Bucket, b: Bucket) => a.name.localeCompare(b.name),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Volume',
    dataIndex: 'volumeName',
    key: 'volumeName',
    sorter: (a: Bucket, b: Bucket) => a.volumeName.localeCompare(b.volumeName),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Owner',
    dataIndex: 'owner',
    key: 'owner',
    sorter: (a: Bucket, b: Bucket) => nullAwareLocaleCompare(a.owner, b.owner)
  },
  {
    title: 'Versioning',
    dataIndex: 'versioning',
    key: 'isVersionEnabled',
    render: (isVersionEnabled: boolean) => renderIsVersionEnabled(isVersionEnabled)
  },
  {
    title: 'Storage Type',
    dataIndex: 'storageType',
    key: 'storageType',
    filterMultiple: true,
    filters: BucketStorageTypeList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Bucket) => record.storageType === value,
    sorter: (a: Bucket, b: Bucket) => a.storageType.localeCompare(b.storageType),
    render: (storageType: BucketStorage) => renderStorageType(storageType)
  },
  {
    title: 'Bucket Layout',
    dataIndex: 'bucketLayout',
    key: 'bucketLayout',
    filterMultiple: true,
    filters: BucketLayoutTypeList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Bucket) => record.bucketLayout === value,
    sorter: (a: Bucket, b: Bucket) => a.bucketLayout.localeCompare(b.bucketLayout),
    render: (bucketLayout: BucketLayout) => renderBucketLayout(bucketLayout)
  },
  {
    title: 'Creation Time',
    dataIndex: 'creationTime',
    key: 'creationTime',
    sorter: (a: Bucket, b: Bucket) => a.creationTime - b.creationTime,
    render: (creationTime: number) => {
      return creationTime > 0 ? moment(creationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Modification Time',
    dataIndex: 'modificationTime',
    key: 'modificationTime',
    sorter: (a: Bucket, b: Bucket) => a.modificationTime - b.modificationTime,
    render: (modificationTime: number) => {
      return modificationTime > 0 ? moment(modificationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Storage Capacity',
    key: 'quotaCapacityBytes',
    sorter: (a: Bucket, b: Bucket) => a.usedBytes - b.usedBytes,
    render: (text: string, record: Bucket) => (
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
    sorter: (a: Bucket, b: Bucket) => a.usedNamespace - b.usedNamespace,
    render: (text: string, record: Bucket) => (
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

const defaultColumns = COLUMNS.map(column => ({
  label: column.title as string,
  value: column.key as string
}));

function getVolumeBucketMap(data: Bucket[]) {
  const volumeBucketMap = data.reduce((
    map: Map<string, Set<Bucket>>,
    currentBucket
  ) => {
    const volume = currentBucket.volumeName;
    if (map.has(volume)) {
      const buckets = Array.from(map.get(volume)!);
      map.set(volume, new Set([...buckets, currentBucket]));
    } else {
      map.set(volume, new Set<Bucket>().add(currentBucket));
    }
    return map;
  }, new Map<string, Set<Bucket>>());
  return volumeBucketMap;
}

function getFilteredBuckets(
  selectedVolumes: Option[],
  bucketsMap: Map<string, Set<Bucket>>
) {
  let selectedBuckets: Bucket[] = [];
  selectedVolumes.forEach(selectedVolume => {
    if (bucketsMap.has(selectedVolume.value)
      && bucketsMap.get(selectedVolume.value)) {
      selectedBuckets = [
        ...selectedBuckets,
        ...Array.from(bucketsMap.get(selectedVolume.value)!)
      ];
    }
  });

  return selectedBuckets;
}

const Buckets: React.FC<{}> = () => {

  let cancelSignal: AbortController;

  const [state, setState] = useState<BucketsState>({
    totalCount: 0,
    lastUpdated: 0,
    columnOptions: defaultColumns,
    volumeBucketMap: new Map<string, Set<Bucket>>(),
    bucketsUnderVolume: [],
    volumeOptions: [],
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedVolumes, setSelectedVolumes] = useState<Option[]>([]);
  const [selectedLimit, setSelectedLimit] = useState<Option>(LIMIT_OPTIONS[0]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [showPanel, setShowPanel] = useState<boolean>(false);
  const [searchColumn, setSearchColumn] = useState<'name' | 'volumeName'>('name');
  const [currentRow, setCurrentRow] = useState<Bucket | Record<string, never>>({})

  const debouncedSearch = useDebounce(searchTerm, 300);
  const { search } = useLocation();

  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} buckets`,
    showSizeChanger: true
  };

  function getVolumeSearchParam() {
    return new URLSearchParams(search).get('volume');
  };

  function getFilteredData(data: Bucket[]) {
    return data.filter(
      (bucket: Bucket) => bucket[searchColumn].includes(debouncedSearch)
    );
  }

  function handleVolumeChange(selected: ValueType<Option, true>) {
    const { volumeBucketMap } = state;
    const volumeSelections = (selected as Option[]);
    let selectedBuckets: Bucket[] = [];

    if (volumeSelections?.length > 0) {
      selectedBuckets = getFilteredBuckets(volumeSelections, volumeBucketMap)
    }

    setSelectedVolumes(volumeSelections);
    setState({
      ...state,
      bucketsUnderVolume: selectedBuckets
    });
  };

  function handleAclLinkClick(bucket: Bucket) {
    setCurrentRow(bucket);
    setShowPanel(true);
  }

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    )
  }

  function addAclColumn() {
    // Inside the class component to access the React internal state
    const aclLinkColumn: ColumnProps<Bucket> = {
      title: 'ACLs',
      dataIndex: 'acls',
      key: 'acls',
      render: (_: any, record: Bucket) => {
        return (
          <a
            key='acl'
            onClick={() => {
              handleAclLinkClick(record);
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
        label: aclLinkColumn.title as string,
        value: aclLinkColumn.key as string
      });
    }
  };

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleLimitChange(selected: ValueType<Option, false>) {
    setSelectedLimit(selected as Option);
  }

  const loadData = () => {
    setLoading(true);
    const { request, controller } = AxiosGetHelper(
      '/api/v1/buckets',
      cancelSignal,
      '',
      { limit: selectedLimit.value }
    );
    cancelSignal = controller;
    request.then(response => {
      const bucketsResponse: BucketResponse = response.data;
      const totalCount = bucketsResponse.totalCount;
      const buckets: Bucket[] = bucketsResponse.buckets;

      const dataSource: Bucket[] = buckets?.map(bucket => {
        return {
          volumeName: bucket.volumeName,
          name: bucket.name,
          versioning: bucket.versioning,
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
      }) ?? [];

      const volumeBucketMap: Map<string, Set<Bucket>> = getVolumeBucketMap(dataSource);

      // Set options for volume selection dropdown
      const volumeOptions: Option[] = Array.from(
        volumeBucketMap.keys()
      ).map(k => ({
        label: k,
        value: k
      }));

      setLoading(false);

      setSelectedVolumes((prevState) => {
        if (prevState.length === 0) return volumeOptions;
        return prevState;
      });

      setState({
        ...state,
        totalCount: totalCount,
        volumeBucketMap: volumeBucketMap,
        volumeOptions: volumeOptions,
        lastUpdated: Number(moment())
      });
    }).catch(error => {
      setLoading(false);
      showDataFetchError(error.toString());
    });
  }

  let autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  useEffect(() => {
    autoReloadHelper.startPolling();
    addAclColumn();
    const initialVolume = getVolumeSearchParam();
    if (initialVolume) {
      setSelectedVolumes([{
        label: initialVolume,
        value: initialVolume
      }]);
    }
    loadData();


    return (() => {
      autoReloadHelper.stopPolling();
      cancelSignal && cancelSignal.abort();
    })
  }, []);

  useEffect(() => {
    // If the data is fetched, we need to regenerate the columns
    // To make sure the filters are properly applied
    setState({
      ...state,
      bucketsUnderVolume: getFilteredBuckets(
        selectedVolumes,
        state.volumeBucketMap
      )
    });
  }, [state.volumeBucketMap])

  // If limit changes, load new data
  useEffect(() => {
    loadData();
  }, [selectedLimit.value]);

  const {
    lastUpdated, columnOptions,
    volumeOptions, bucketsUnderVolume
  } = state;

  return (
    <>
      <div className='page-header-v2'>
        Buckets
        <AutoReloadPanel
          isLoading={loading}
          lastRefreshed={lastUpdated}
          togglePolling={autoReloadHelper.handleAutoReloadToggle}
          onReload={loadData}
        />
      </div>
      <div style={{ padding: '24px' }}>
        <div className='content-div'>
          <div className='table-header-section'>
            <div className='table-filter-section'>
              <MultiSelect
                options={volumeOptions}
                defaultValue={selectedVolumes}
                selected={selectedVolumes}
                placeholder='Volumes'
                onChange={handleVolumeChange}
                fixedColumn=''
                onTagClose={() => { }}
                columnLength={volumeOptions.length} />
              <MultiSelect
                options={columnOptions}
                defaultValue={selectedColumns}
                selected={selectedColumns}
                placeholder='Columns'
                onChange={handleColumnChange}
                onTagClose={() => { }}
                fixedColumn='name'
                columnLength={COLUMNS.length} />
              <SingleSelect
                options={LIMIT_OPTIONS}
                defaultValue={selectedLimit}
                placeholder='Limit'
                onChange={handleLimitChange} />
            </div>
            <Search
              disabled={bucketsUnderVolume?.length < 1}
              searchOptions={SearchableColumnOpts}
              searchInput={searchTerm}
              searchColumn={searchColumn}
              onSearchChange={
                (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
              }
              onChange={(value) => {
                setSearchTerm('');
                setSearchColumn(value as 'name' | 'volumeName');
              }} />
          </div>
          <div>
            <Table
              dataSource={getFilteredData(bucketsUnderVolume)}
              columns={filterSelectedColumns()}
              loading={loading}
              rowKey='volume'
              pagination={paginationConfig}
              scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
              locale={{ filterTitle: '' }}
            />
          </div>
        </div>
        <AclPanel
          visible={showPanel}
          acls={currentRow.acls}
          entityName={currentRow.name}
          entityType='Bucket'
          onClose={() => setShowPanel(false)} />
      </div>
    </>
  )
}

export default Buckets;