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
import { Container, DeletedContainerKeysResponse, Pipelines } from '@/v2/types/insights.types';
import Table, { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
import { showDataFetchError } from '@/utils/common';

//------Types-------
type DeletedContainerKeysTableProps = {
  paginationConfig: TablePaginationConfig;
  limit: string;
  onRowExpand: () => void;
  expandedRowRender: (arg0: any) => JSX.Element;
}

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
  expandedRowRender,
  onRowExpand,
  paginationConfig,
  limit = '1000'
}) => {

  const [loading, setLoading] = React.useState<boolean>(false);
  const [data, setData] = React.useState<Container[]>();

  const cancelSignal = React.useRef<AbortController>();

  function fetchDeletedKeys() {
    const { request, controller } = AxiosGetHelper(
      `/api/v1/containers/mismatch/deleted?limit=${limit}`,
      cancelSignal.current
    )
    cancelSignal.current = controller;

    request.then(response => {
      setLoading(true);
      const deletedContainerKeys: DeletedContainerKeysResponse = response?.data;
      setData(deletedContainerKeys?.containers ?? []);
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    });
  }

  React.useEffect(() => {
    fetchDeletedKeys();

    return(() => {
      cancelSignal.current && cancelSignal.current.abort();
    })
  }, []);


  return (
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
      locale={{ filterTitle: '' }} />
  )
}

export default DeletedContainerKeysTable;