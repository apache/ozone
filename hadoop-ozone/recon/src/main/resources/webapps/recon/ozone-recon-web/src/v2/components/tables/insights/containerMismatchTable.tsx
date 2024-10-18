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
  Popover,
  Table
} from 'antd';
import {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import {
  MenuProps as FilterMenuProps
} from 'antd/es/menu';
import { FilterFilled } from '@ant-design/icons';
import { ValueType } from 'react-select';

import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import { showDataFetchError } from '@/utils/common';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
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


const ContainerMismatchTable: React.FC<ContainerMismatchTableProps> = ({
  paginationConfig,
  limit,
  onRowExpand,
  expandedRowRender,
  handleLimitChange
}) => {

  const [loading, setLoading] = React.useState<boolean>(false);
  const [data, setData] = React.useState<Container[]>();

  const cancelSignal = React.useRef<AbortController>();

  const handleExistAtChange: FilterMenuProps['onClick'] = ({ key }) => {
    if (key === 'OM') {
      fetchMismatchContainers('SCM');
    } else {
      fetchMismatchContainers('OM');
    }
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
          <label> Exists At&nbsp;&nbsp;&nbsp;&nbsp;<FilterFilled /> </label>
        </Dropdown>
      </>,
      dataIndex: 'existsAt'
    }
  ];

  function fetchMismatchContainers(missingIn: string) {
    setLoading(true);
    const { request, controller } = AxiosGetHelper(
      `/api/v1/containers/mismatch?limit=${limit.value}&missingIn=${missingIn}`,
      cancelSignal.current
    );

    cancelSignal.current = controller;
    request.then(response => {
      const mismatchedContainers: MismatchContainersResponse = response?.data;
      setData(mismatchedContainers?.containerDiscrepancyInfo ?? []);
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    })
  }

  React.useEffect(() => {
    //Fetch containers missing in OM by default
    fetchMismatchContainers('OM');

    return (() => {
      cancelSignal.current && cancelSignal.current.abort();
    })
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
      </div>
      <Table
        expandable={{
          expandRowByClick: true,
          expandedRowRender: expandedRowRender,
          onExpand: onRowExpand
        }}
        dataSource={data}
        columns={COLUMNS}
        loading={loading}
        pagination={paginationConfig}
        rowKey='containerId'
        locale={{ filterTitle: '' }}
        scroll={{ x: 'max-content' }}  />
    </>
  )
}

export default ContainerMismatchTable;