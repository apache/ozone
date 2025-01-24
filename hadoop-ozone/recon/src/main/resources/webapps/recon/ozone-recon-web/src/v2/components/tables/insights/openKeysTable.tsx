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
import {
  Dropdown,
  Menu,
  Table
} from 'antd';
import {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import { MenuProps } from 'antd/es/menu';
import { FilterFilled } from '@ant-design/icons';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
import { byteToSize, showDataFetchError } from '@/utils/common';
import { getFormattedTime } from '@/v2/utils/momentUtils';
import { useDebounce } from '@/v2/hooks/debounce.hook';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import { OpenKeys, OpenKeysResponse } from '@/v2/types/insights.types';


//--------Types--------
type OpenKeysTableProps = {
  limit: Option;
  paginationConfig: TablePaginationConfig;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
}

//-----Components------
const OpenKeysTable: React.FC<OpenKeysTableProps> = ({
  limit,
  paginationConfig,
  handleLimitChange
}) => {
  const [loading, setLoading] = React.useState<boolean>(false);
  const [data, setData] = React.useState<OpenKeys[]>();
  const [searchTerm, setSearchTerm] = React.useState<string>('');

  const cancelSignal = React.useRef<AbortController>();
  const debouncedSearch = useDebounce(searchTerm, 300);

  function filterData(data: OpenKeys[] | undefined) {
    return data?.filter(
      (data: OpenKeys) => data.path.includes(debouncedSearch)
    );
  }

  function fetchOpenKeys(isFso: boolean) {
    setLoading(true);

    const { request, controller } = AxiosGetHelper(
      `/api/v1/keys/open?includeFso=${isFso}&includeNonFso=${!isFso}&limit=${limit.value}`,
      cancelSignal.current
    );
    cancelSignal.current = controller;

    request.then(response => {
      const openKeys: OpenKeysResponse = response?.data ?? { 'fso': [] };
      let allOpenKeys: OpenKeys[];
      if (isFso) {
        allOpenKeys = openKeys['fso']?.map((key: OpenKeys) => ({
          ...key,
          type: 'FSO'
        })) ?? [];
      } else {
        allOpenKeys = openKeys['nonFSO']?.map((key: OpenKeys) => ({
          ...key,
          type: 'Non FSO'
        })) ?? [];
      }

      setData(allOpenKeys);
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    });
  }

  const handleKeyTypeChange: MenuProps['onClick'] = (e) => {
    if (e.key === 'fso') {
      fetchOpenKeys(true);
    } else {
      fetchOpenKeys(false);
    }
  }

  const COLUMNS: ColumnsType<OpenKeys> = [{
    title: 'Key Name',
    dataIndex: 'path',
    key: 'path'
  },
  {
    title: 'Size',
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
      return getFormattedTime(inStateSince, 'll LTS');
    }
  },
  {
    title: 'Replication Factor',
    dataIndex: 'replicationInfo',
    key: 'replicationfactor',
    render: (replicationInfo: any) => (
      <div>
        {Object.values(replicationInfo)[0]}
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
  }, {
    title: <>
      <Dropdown
        overlay={
          <Menu onClick={handleKeyTypeChange}>
            <Menu.Item key='fso'>FSO</Menu.Item>
            <Menu.Item key='nonFSO'> Non-FSO</Menu.Item>
          </Menu>
        }>
        <label> Key Type&nbsp;&nbsp;&nbsp;&nbsp;<FilterFilled /></label>
      </Dropdown>
    </>,
    dataIndex: 'type',
    key: 'type',
    render: (type: string) => <div key={type}>{type}</div>
  }];

  React.useEffect(() => {
    // Fetch FSO open keys by default
    fetchOpenKeys(true);

    return (() => cancelSignal.current && cancelSignal.current.abort());
  }, [limit.value]);

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
        dataSource={filterData(data)}
        columns={COLUMNS}
        loading={loading}
        rowKey='key'
        pagination={paginationConfig}
        locale={{ filterTitle: '' }}
        scroll={{ x: 'max-content' }} />
    </>
  );
}

export default OpenKeysTable;