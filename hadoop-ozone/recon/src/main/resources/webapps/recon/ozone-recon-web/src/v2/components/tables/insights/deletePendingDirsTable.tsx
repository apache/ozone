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
import { AxiosError } from 'axios';
import Table, {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
import { byteToSize, showDataFetchError } from '@/utils/common';
import { getFormattedTime } from '@/v2/utils/momentUtils';
import { useDebounce } from '@/v2/hooks/debounce.hook';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import { DeletedDirInfo } from '@/v2/types/insights.types';

//-----Types------
type DeletePendingDirTableProps = {
  paginationConfig: TablePaginationConfig
  limit: Option;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
}

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

  const [loading, setLoading] = React.useState<boolean>(false);
  const [data, setData] = React.useState<DeletedDirInfo[]>();
  const [searchTerm, setSearchTerm] = React.useState<string>('');

  const cancelSignal = React.useRef<AbortController>();
  const debouncedSearch = useDebounce(searchTerm, 300);

  function filterData(data: DeletedDirInfo[] | undefined) {
    return data?.filter(
      (data: DeletedDirInfo) => data.key.includes(debouncedSearch)
    );
  }

  function loadData() {
    setLoading(true);

    const { request, controller } = AxiosGetHelper(
      `/api/v1/keys/deletePending/dirs?limit=${limit.value}`,
      cancelSignal.current
    );
    cancelSignal.current = controller;

    request.then(response => {
      setData(response?.data?.deletedDirInfo ?? []);
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    });
  }

  React.useEffect(() => {
    loadData();

    return (() => cancelSignal.current && cancelSignal.current.abort());
  }, [limit.value]);

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
      loading={loading}
      dataSource={filterData(data)}
      columns={COLUMNS}
      pagination={paginationConfig}
      rowKey='key'
      locale={{ filterTitle: '' }}
      scroll={{ x: 'max-content' }} />
  </>)
}

export default DeletePendingDirTable;