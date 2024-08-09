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

import React, { useEffect, useState } from 'react';
import moment from 'moment';
import { Table } from 'antd';
import { Link } from 'react-router-dom';
import {
  TablePaginationConfig,
  ColumnsType
} from 'antd/es/table';
import { ValueType } from 'react-select/src/types';

import QuotaBar from '@/components/quotaBar/quotaBar';
import { AclPanel } from '@/components/aclDrawer/aclDrawer';
import AutoReloadPanel from '@/components/autoReloadPanel/autoReloadPanel';
import MultiSelect, { Option } from '@/v2/components/select/multiSelect';

import { byteToSize, showDataFetchError } from '@/utils/common';
import { AutoReloadHelper } from '@/utils/autoReloadHelper';
import { AxiosGetHelper } from "@/utils/axiosRequestHelper";

import { Volume, VolumesState, VolumesResponse } from '@/v2/types/volume.types';

import './volumes.less';
import SingleSelect from '@/v2/components/select/singleSelect';
import Search from '@/v2/components/search/search';

const SearchableColumnOpts = [
  {
    label: 'Volume',
    value: 'volume'
  },
  {
    label: 'Owner',
    value: 'owner'
  }
]

const LIMIT_OPTIONS: Option[] = [
  { label: '1000', value: '1000' },
  { label: '5000', value: "5000" },
  { label: '10000', value: "10000" },
  { label: '20000', value: "20000" }
]

const Volumes: React.FC<{}> = () => {

  let cancelSignal: AbortController;

  const COLUMNS: ColumnsType<Volume> = [
    {
      title: 'Volume',
      dataIndex: 'volume',
      key: 'volume',
      sorter: (a: Volume, b: Volume) => a.volume.localeCompare(b.volume),
      defaultSortOrder: 'ascend' as const,
      width: '15%'
    },
    {
      title: 'Owner',
      dataIndex: 'owner',
      key: 'owner',
      sorter: (a: Volume, b: Volume) => a.owner.localeCompare(b.owner)
    },
    {
      title: 'Admin',
      dataIndex: 'admin',
      key: 'admin',
      sorter: (a: Volume, b: Volume) => a.admin.localeCompare(b.admin)
    },
    {
      title: 'Creation Time',
      dataIndex: 'creationTime',
      key: 'creationTime',
      sorter: (a: Volume, b: Volume) => a.creationTime - b.creationTime,
      render: (creationTime: number) => {
        return creationTime > 0 ? moment(creationTime).format('ll LTS') : 'NA';
      }
    },
    {
      title: 'Modification Time',
      dataIndex: 'modificationTime',
      key: 'modificationTime',
      sorter: (a: Volume, b: Volume) => a.modificationTime - b.modificationTime,
      render: (modificationTime: number) => {
        return modificationTime > 0 ? moment(modificationTime).format('ll LTS') : 'NA';
      }
    },
    {
      title: 'Quota (Size)',
      dataIndex: 'quotaInBytes',
      key: 'quotaInBytes',
      render: (quotaInBytes: number) => {
        return quotaInBytes && quotaInBytes !== -1 ? byteToSize(quotaInBytes, 3) : 'NA';
      }
    },
    {
      title: 'Namespace Capacity',
      key: 'namespaceCapacity',
      sorter: (a: Volume, b: Volume) => a.usedNamespace - b.usedNamespace,
      render: (text: string, record: Volume) => (
        <QuotaBar
          quota={record.quotaInNamespace}
          used={record.usedNamespace}
          quotaType='namespace'
        />
      )
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: Volume) => {
        const searchParams = new URLSearchParams();
        searchParams.append('volume', record.volume);

        return (
          <>
            <Link
              key="listBuckets"
              to={`/Buckets?${searchParams.toString()}`}
              style={{
                marginRight: '16px'
              }}>
              Show buckets
            </Link>
            <a
              key='acl'
              onClick={() => handleAclLinkClick(record)}>
              Show ACL
            </a>
          </>
        );
      }
    }
  ];

  const defaultColumns = COLUMNS.map(column => ({
    label: column.title as string,
    value: column.key as string,
  }));

  const [state, setState] = useState<VolumesState>({
    data: [],
    totalCount: 0,
    lastUpdated: 0,
    columnOptions: defaultColumns,
    showPanel: false,
    currentRow: {}
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [selectedColumns, setSelectedColumns] = useState<Option[]>(defaultColumns);
  const [selectedLimit, setSelectedLimit] = useState<Option>(LIMIT_OPTIONS[0]);
  const [searchColumn, setSearchColumn] = useState<'volume' | 'owner'>('volume');
  const [searchTerm, setSearchTerm] = useState<string>('');

  const loadData = () => {
    setLoading(true);

    const { request, controller } = AxiosGetHelper(
      '/api/v1/volumes',
      cancelSignal,
      "",
      { limit: selectedLimit.value }
    );

    cancelSignal = controller;
    request.then(response => {
      const volumesResponse: VolumesResponse = response.data;
      const totalCount = volumesResponse.totalCount;
      const volumes: Volume[] = volumesResponse.volumes;
      const data: Volume[] = volumes.map(volume => {
        return {
          volume: volume.volume,
          owner: volume.owner,
          admin: volume.admin,
          creationTime: volume.creationTime,
          modificationTime: volume.modificationTime,
          quotaInBytes: volume.quotaInBytes,
          quotaInNamespace: volume.quotaInNamespace,
          usedNamespace: volume.usedNamespace,
          acls: volume.acls
        };
      });

      setState({
        ...state,
        data,
        totalCount,
        lastUpdated: Number(moment()),
        showPanel: false
      });
      setLoading(false);
    }).catch(error => {
      setState({
        ...state,
        showPanel: false
      });
      setLoading(false);
      showDataFetchError(error.toString());
    });
  };

  let autoReloadHelper: AutoReloadHelper = new AutoReloadHelper(loadData);

  useEffect(() => {
    loadData();
    autoReloadHelper.startPolling();

    // Component will unmount
    return (() => {
      autoReloadHelper.stopPolling();
      cancelSignal && cancelSignal.abort();
    })
  }, []);

  // If limit changes, load new data
  useEffect(() => {
    loadData();
  }, [selectedLimit.value]);

  function handleColumnChange(selected: ValueType<Option, true>) {
    setSelectedColumns(selected as Option[]);
  }

  function handleLimitChange(selected: ValueType<Option, false>) {
    setSelectedLimit(selected as Option);
  }

  function handleTagClose(label: string) {
    setSelectedColumns(
      selectedColumns.filter((column) => column.label !== label)
    )
  }


  function handleAclLinkClick(volume: Volume) {
    setState({
      ...state,
      showPanel: true,
      currentRow: volume
    })
  }

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    )
  }

  function getFilteredData(data: Volume[]) {
    return data.filter((volume: Volume) => volume[searchColumn].includes(searchTerm))
  }


  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} volumes`,
    showSizeChanger: true
  };

  const {
    data, totalCount,
    lastUpdated,
    columnOptions, showPanel,
    currentRow } = state;

  return (
    <>
      <div className='page-header-v2'>
        Volumes
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
                options={columnOptions}
                defaultValue={selectedColumns}
                selected={selectedColumns}
                placeholder='Columns'
                onChange={handleColumnChange}
                onTagClose={handleTagClose}
                fixedColumn='Volume'
                isOptionDisabled={(option) => option.value === 'volume'}
                columnLength={COLUMNS.length} />
              <SingleSelect
                options={LIMIT_OPTIONS}
                defaultValue={selectedLimit}
                placeholder='Limit'
                onChange={handleLimitChange} />
            </div>
            <Search
              searchOptions={SearchableColumnOpts}
              searchColumn={searchColumn}
              onSearch={(value) => setSearchTerm(value)}
              onChange={(value) => {
                setSearchTerm('');
                setSearchColumn(value as 'volume' | 'owner');
              }} />
          </div>
          <div>
            <Table
              dataSource={getFilteredData(data)}
              columns={filterSelectedColumns()}
              loading={loading}
              rowKey='volume'
              pagination={paginationConfig}
              scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
              locale={{ filterTitle: '' }}
            />
          </div>
        </div>
        <AclPanel visible={showPanel} acls={currentRow.acls} objName={currentRow.volume} objType='Volume' />
      </div>
    </>
  );
}

export default Volumes;
