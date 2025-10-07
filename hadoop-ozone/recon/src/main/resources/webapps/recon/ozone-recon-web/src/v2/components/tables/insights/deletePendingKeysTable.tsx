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
import ExpandedPendingKeysTable from '@/v2/components/tables/insights/expandedPendingKeysTable';
import { byteToSize, showDataFetchError } from '@/utils/common';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import {
  DeletePendingKey,
  DeletePendingKeysResponse
} from '@/v2/types/insights.types';

//-----Types------
type DeletePendingKeysTableProps = {
  paginationConfig: TablePaginationConfig
  limit: Option;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
}

type DeletePendingKeysColumns = {
  fileName: string;
  keyName: string;
  dataSize: number;
  keyCount: number;
}

type ExpandedDeletePendingKeys = {
  omKeyInfoList: DeletePendingKey[]
}

const DEFAULT_DELETE_PENDING_KEYS_RESPONSE: DeletePendingKeysResponse = {
  deletedKeyInfo: []
};

//------Constants------
const COLUMNS: ColumnsType<DeletePendingKeysColumns> = [
  {
    title: 'Key Name',
    dataIndex: 'fileName',
    key: 'fileName'
  },
  {
    title: 'Path',
    dataIndex: 'keyName',
    key: 'keyName',
  },
  {
    title: 'Total Data Size',
    dataIndex: 'dataSize',
    key: 'dataSize',
    render: (dataSize: number) => byteToSize(dataSize, 1)
  },
  {
    title: 'Total Key Count',
    dataIndex: 'keyCount',
    key: 'keyCount',
  }
];

//-----Components------
const DeletePendingKeysTable: React.FC<DeletePendingKeysTableProps> = ({
  paginationConfig,
  limit,
  handleLimitChange
}) => {
  const [data, setData] = useState<DeletePendingKeysColumns[]>([]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [expandedDeletePendingKeys, setExpandedDeletePendingKeys] = useState<ExpandedDeletePendingKeys[]>([]);

  const debouncedSearch = useDebounce(searchTerm, 300);

  // Use the modern hooks pattern
  const deletePendingKeysData = useApiData<DeletePendingKeysResponse>(
    `/api/v1/keys/deletePending?limit=${limit.value}`,
    DEFAULT_DELETE_PENDING_KEYS_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  // Process data when it changes
  useEffect(() => {
    if (deletePendingKeysData.data && deletePendingKeysData.data.deletedKeyInfo) {
      const deletePendingKeys = deletePendingKeysData.data;
      let deletedKeyData: DeletePendingKeysColumns[] = [];
      let expandedData: ExpandedDeletePendingKeys[] = [];

      // Sum up the data size and organize related key information
      deletedKeyData = deletePendingKeys.deletedKeyInfo?.flatMap((keyInfo) => {
        expandedData.push(keyInfo);
        let count = 0;
        let item: DeletePendingKey = keyInfo.omKeyInfoList?.reduce((obj, curr) => {
          count += 1;
          return { ...curr, dataSize: obj.dataSize + curr.dataSize };
        }, { ...keyInfo.omKeyInfoList[0], dataSize: 0 });

        return {
          dataSize: item.dataSize,
          fileName: item.fileName,
          keyName: item.keyName,
          path: item.path,
          keyCount: count
        }
      }) || [];

      setData(deletedKeyData);
      setExpandedDeletePendingKeys(expandedData);
    }
  }, [deletePendingKeysData.data]);

  // Refetch when limit changes
  useEffect(() => {
    deletePendingKeysData.refetch();
  }, [limit.value]);

  function filterData(data: DeletePendingKeysColumns[] | undefined) {
    return data?.filter(
      (data: DeletePendingKeysColumns) => data.keyName.includes(debouncedSearch)
    );
  }

  function expandedRowRender(record: DeletePendingKeysColumns) {
    const filteredData = expandedDeletePendingKeys?.flatMap((info) => (
      info.omKeyInfoList?.filter((key) => key.keyName === record.keyName)
    ));
    return (
      <ExpandedPendingKeysTable
        data={filteredData}
        paginationConfig={paginationConfig} />
    )
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
          expandedRowRender: expandedRowRender
        }}
        dataSource={filterData(data)}
        columns={COLUMNS}
        loading={deletePendingKeysData.loading}
        pagination={paginationConfig}
        rowKey='keyName'
        locale={{ filterTitle: '' }}
        scroll={{ x: 'max-content' }} />
    </>
  )
}

export default DeletePendingKeysTable;
