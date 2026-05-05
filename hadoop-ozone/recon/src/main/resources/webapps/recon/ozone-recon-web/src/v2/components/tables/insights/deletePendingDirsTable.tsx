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
import { byteToSize, showDataFetchError } from '@/utils/common';
import { getFormattedTime } from '@/v2/utils/momentUtils';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import { DeletedDirInfo } from '@/v2/types/insights.types';

//-----Types------
type DeletePendingDirTableProps = {
  paginationConfig: TablePaginationConfig
  limit: Option;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
}

const DEFAULT_DELETE_PENDING_DIRS_RESPONSE = {
  deletedDirInfo: []
};

//-----Constants------
const COLUMNS: ColumnsType<DeletedDirInfo> = [{
  title: 'Directory Name',
  dataIndex: 'key',
  key: 'key'
},
{
  title: 'In state since',
  dataIndex: 'inStateSince',
  key: 'inStateSince',
  render: (inStateSince: number) => {
    return getFormattedTime(inStateSince, 'll LTS');
  }
},
{
  title: 'Path',
  dataIndex: 'path',
  key: 'path'
},
{
  title: 'Size',
  dataIndex: 'size',
  key: 'size',
  render: (dataSize: number) => byteToSize(dataSize, 1)
}];

//-----Components------
const DeletePendingDirTable: React.FC<DeletePendingDirTableProps> = ({
  limit,
  paginationConfig,
  handleLimitChange
}) => {
  const [data, setData] = useState<DeletedDirInfo[]>([]);
  const [searchTerm, setSearchTerm] = useState<string>('');

  const debouncedSearch = useDebounce(searchTerm, 300);

  // Use the modern hooks pattern
  const deletePendingDirsData = useApiData<{ deletedDirInfo: DeletedDirInfo[] }>(
    `/api/v1/keys/deletePending/dirs?limit=${limit.value}`,
    DEFAULT_DELETE_PENDING_DIRS_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  // Process data when it changes
  useEffect(() => {
    if (deletePendingDirsData.data && deletePendingDirsData.data.deletedDirInfo) {
      setData(deletePendingDirsData.data.deletedDirInfo);
    }
  }, [deletePendingDirsData.data]);

  // Refetch when limit changes
  useEffect(() => {
    deletePendingDirsData.refetch();
  }, [limit.value]);

  function filterData(data: DeletedDirInfo[] | undefined) {
    return data?.filter(
      (data: DeletedDirInfo) => data.key.includes(debouncedSearch)
    );
  }

  return (<>
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
      loading={deletePendingDirsData.loading}
      dataSource={filterData(data)}
      columns={COLUMNS}
      pagination={paginationConfig}
      rowKey='key'
      locale={{ filterTitle: '' }}
      scroll={{ x: 'max-content' }} />
  </>)
}

export default DeletePendingDirTable;
