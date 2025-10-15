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

import React, { useState, useEffect } from 'react';
import Table, {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import { showDataFetchError } from '@/utils/common';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import {
  Container,
  DeletedContainerKeysResponse,
  Pipelines
} from '@/v2/types/insights.types';

//------Types-------
type DeletedContainerKeysTableProps = {
  paginationConfig: TablePaginationConfig;
  limit: Option;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
  onRowExpand: (arg0: boolean, arg1: any) => void;
  expandedRowRender: (arg0: any) => JSX.Element;
}

const DEFAULT_DELETED_CONTAINER_KEYS_RESPONSE: DeletedContainerKeysResponse = {
  containers: []
};

//------Constants------
const COLUMNS: ColumnsType<Container> = [
  {
    title: 'Container ID',
    dataIndex: 'containerId',
    key: 'containerId',
    width: '20%'
  },
  {
    title: 'Count Of Keys',
    dataIndex: 'numberOfKeys',
    key: 'numberOfKeys',
    sorter: (a: Container, b: Container) => a.numberOfKeys - b.numberOfKeys
  },
  {
    title: 'Pipelines',
    dataIndex: 'pipelines',
    key: 'pipelines',
    render: (pipelines: Pipelines[]) => (
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

//-----Components------
const DeletedContainerKeysTable: React.FC<DeletedContainerKeysTableProps> = ({
  limit,
  paginationConfig,
  handleLimitChange,
  onRowExpand,
  expandedRowRender
}) => {
  const [data, setData] = useState<Container[]>([]);
  const [searchTerm, setSearchTerm] = useState<string>('');

  const debouncedSearch = useDebounce(searchTerm, 300);

  // Use the modern hooks pattern
  const deletedContainerKeysData = useApiData<DeletedContainerKeysResponse>(
    `/api/v1/containers/mismatch/deleted?limit=${limit.value}`,
    DEFAULT_DELETED_CONTAINER_KEYS_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  // Process data when it changes
  useEffect(() => {
    if (deletedContainerKeysData.data && deletedContainerKeysData.data.containers) {
      setData(deletedContainerKeysData.data.containers);
    }
  }, [deletedContainerKeysData.data]);

  // Refetch when limit changes
  useEffect(() => {
    deletedContainerKeysData.refetch();
  }, [limit.value]);

  function filterData(data: Container[] | undefined) {
    return data?.filter(
      (data: Container) => data.containerId.toString().includes(debouncedSearch)
    );
  }

  return (
    <>
      <div className='table-header-section'>
        <div className='table-filter-section'>
          <SingleSelect
            options={LIMIT_OPTIONS}
            defaultValue={limit}
            placeholder='Limit'
            onChange={handleLimitChange} />
        </div>
        <Search
          disabled={(data?.length ?? 0) < 1}
          searchInput={searchTerm}
          onSearchChange={
            (e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)
          }
          onChange={() => { }} />
      </div>
      <Table
        expandable={{
          expandRowByClick: true,
          expandedRowRender: expandedRowRender,
          onExpand: onRowExpand
        }}
        dataSource={filterData(data)}
        columns={COLUMNS}
        loading={deletedContainerKeysData.loading}
        pagination={paginationConfig}
        rowKey='containerId'
        locale={{ filterTitle: '' }}
        scroll={{ x: 'max-content' }} />
    </>
  )
}

export default DeletedContainerKeysTable;
