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
import {
  Dropdown,
  Menu,
  Popover,
  Table,
  Tooltip
} from 'antd';
import {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import {
  MenuProps as FilterMenuProps
} from 'antd/es/menu';
import { FilterFilled, InfoCircleOutlined } from '@ant-design/icons';
import { ValueType } from 'react-select';

import Search from '@/v2/components/search/search';
import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import { showDataFetchError } from '@/utils/common';
import { useApiData } from '@/v2/hooks/useAPIData.hook';
import { useDebounce } from '@/v2/hooks/useDebounce';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import {
  Container,
  MismatchContainersResponse,
  Pipelines
} from '@/v2/types/insights.types';

//-----Types-----
type ContainerMismatchTableProps = {
  paginationConfig: TablePaginationConfig;
  limit: Option;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
  expandedRowRender: (arg0: any) => JSX.Element;
  onRowExpand: (arg0: boolean, arg1: any) => void;
}

const DEFAULT_MISMATCH_RESPONSE: MismatchContainersResponse = {
  containerDiscrepancyInfo: []
};

//-----Components------
const ContainerMismatchTable: React.FC<ContainerMismatchTableProps> = ({
  paginationConfig,
  limit,
  onRowExpand,
  expandedRowRender,
  handleLimitChange
}) => {
  const [data, setData] = useState<Container[]>([]);
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [missingIn, setMissingIn] = useState<string>('OM');

  const debouncedSearch = useDebounce(searchTerm, 300);

  // Use the modern hooks pattern
  const mismatchData = useApiData<MismatchContainersResponse>(
    `/api/v1/containers/mismatch?limit=${limit.value}&missingIn=${missingIn}`,
    DEFAULT_MISMATCH_RESPONSE,
    {
      retryAttempts: 2,
      initialFetch: false,
      onError: (error) => showDataFetchError(error)
    }
  );

  // Process data when it changes
  useEffect(() => {
    if (mismatchData.data && mismatchData.data.containerDiscrepancyInfo) {
      setData(mismatchData.data.containerDiscrepancyInfo);
    }
  }, [mismatchData.data]);

  // Refetch when limit or missingIn changes
  useEffect(() => {
    mismatchData.refetch();
  }, [limit.value, missingIn]);

  const handleExistAtChange: FilterMenuProps['onClick'] = ({ key }) => {
    if (key === 'OM') {
      setMissingIn('SCM');
    } else {
      setMissingIn('OM');
    }
  }

  function filterData(data: Container[] | undefined) {
    return data?.filter(
      (data: Container) => data.containerId.toString().includes(debouncedSearch)
    );
  }

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
      render: (pipelines: Pipelines[]) => {
        const renderPipelineIds = (pipelineIds: Pipelines[]) => {
          return pipelineIds?.map(pipeline => (
            <div key={pipeline.id.id}>
              {pipeline.id.id}
            </div>
          ));
        }
        return (
          <Popover
            content={
              renderPipelineIds(pipelines)
            }
            title='Related Pipelines'
            placement='bottomLeft'
            trigger='hover'>
            <strong>{pipelines.length}</strong> pipelines
          </Popover>
        )
      }
    },
    {
      title: <>
        <Dropdown
          overlay={
            <Menu onClick={handleExistAtChange}>
              <Menu.Item key='OM'> OM </Menu.Item>
              <Menu.Item key='SCM'> SCM </Menu.Item>
            </Menu>
          }>
          <label style={{ marginRight: '5%' }}> Exists At&nbsp;&nbsp;&nbsp;<FilterFilled /> </label>
        </Dropdown>
        <Tooltip placement='top' title={
          <span>
            <strong>SCM</strong>: Container exists at SCM but missing at OM.<br />
            <strong>OM</strong>: Container exist at OM but missing at SCM.
          </span>}>
          <InfoCircleOutlined />
        </Tooltip>
      </>,
      dataIndex: 'existsAt'
    }
  ];

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
        loading={mismatchData.loading}
        pagination={paginationConfig}
        rowKey='containerId'
        locale={{ filterTitle: '' }}
        scroll={{ x: 'max-content' }} />
    </>
  )
}

export default ContainerMismatchTable;
