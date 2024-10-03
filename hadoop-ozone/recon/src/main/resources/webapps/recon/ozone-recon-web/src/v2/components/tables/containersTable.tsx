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

import React, { useState, useRef } from 'react';
import moment from 'moment';
import filesize from 'filesize';
import Table, {
  ColumnProps,
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import {
  Container, ContainerKeysResponse, ContainerReplicas,
  ContainerTableProps,
  ExpandedRow, ExpandedRowState, KeyResponse
} from '@/v2/types/container.types';
import { getFormattedTime } from '@/v2/utils/momentUtils';
import { NodeIndexOutlined } from '@ant-design/icons';
import { Popover } from 'antd';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
import { showDataFetchError } from '@/utils/common';
import { AxiosError } from 'axios';

const size = filesize.partial({ standard: 'iec' });

export const COLUMNS: ColumnsType<Container> = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: Container, b: Container) => a.containerID - b.containerID
  },
  {
    title: 'No. of Keys',
    dataIndex: 'keys',
    key: 'keys',
    sorter: (a: Container, b: Container) => a.keys - b.keys
  },
  {
    title: 'Actual/Expected Replica(s)',
    dataIndex: 'expectedReplicaCount',
    key: 'expectedReplicaCount',
    render: (expectedReplicaCount: number, record: Container) => {
      const actualReplicaCount = record.actualReplicaCount;
      return (
        <span>
          {actualReplicaCount} / {expectedReplicaCount}
        </span>
      );
    }
  },
  {
    title: 'Datanodes',
    dataIndex: 'replicas',
    key: 'replicas',
    render: (replicas: ContainerReplicas[]) => {
      const renderDatanodes = (replicas: ContainerReplicas[]) => {
        return replicas?.map((replica: any, idx: number) => (
          <div key={idx} className='datanode-container-v2'>
            <NodeIndexOutlined /> {replica.datanodeHost}
          </div>
        ))
      }

      return (
        <Popover
          content={renderDatanodes(replicas)}
          title='Datanodes'
          placement='bottomRight'
          trigger='hover'>
          <strong>{replicas.length}</strong> datanodes
        </Popover>
      )
    }
  },
  {
    title: 'Pipeline ID',
    dataIndex: 'pipelineID',
    key: 'pipelineID'
  },
  {
    title: 'Unhealthy Since',
    dataIndex: 'unhealthySince',
    key: 'unhealthySince',
    render: (unhealthySince: number) => getFormattedTime(unhealthySince, 'lll'),
    sorter: (a: Container, b: Container) => a.unhealthySince - b.unhealthySince
  }
];

const KEY_TABLE_COLUMNS: ColumnsType<KeyResponse> = [
  {
    title: 'Volume',
    dataIndex: 'Volume',
    key: 'Volume'
  },
  {
    title: 'Bucket',
    dataIndex: 'Bucket',
    key: 'Bucket'
  },
  {
    title: 'Key',
    dataIndex: 'Key',
    key: 'Key'
  },
  {
    title: 'Size',
    dataIndex: 'DataSize',
    key: 'DataSize',
    render: (dataSize: number) => <div>{size(dataSize)}</div>
  },
  {
    title: 'Date Created',
    dataIndex: 'CreationTime',
    key: 'CreationTime',
    render: (date: string) => getFormattedTime(date, 'lll')
  },
  {
    title: 'Date Modified',
    dataIndex: 'ModificationTime',
    key: 'ModificationTime',
    render: (date: string) => getFormattedTime(date, 'lll')
  }
];

const ContainerTable: React.FC<ContainerTableProps> = ({
  data,
  loading,
  selectedColumns,
  expandedRow,
  expandedRowSetter,
  searchColumn = 'containerID',
  searchTerm = ''
}) => {

  const cancelSignal = useRef<AbortController>();

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    );
  }

  function loadRowData(containerID: number) {
    const { request, controller } = AxiosGetHelper(
      `/api/v1/containers/${containerID}/keys`,
      cancelSignal.current
    );
    cancelSignal.current = controller;

    request.then(response => {
      const containerKeysResponse: ContainerKeysResponse = response.data;
      expandedRowSetter({
        ...expandedRow,
        [containerID]: {
          ...expandedRow[containerID],
          loading: false,
          dataSource: containerKeysResponse.keys,
          totalCount: containerKeysResponse.totalCount
        }
      });
    }).catch(error => {
      expandedRowSetter({
        ...expandedRow,
        [containerID]: {
          ...expandedRow[containerID],
          loading: false
        }
      });
      showDataFetchError((error as AxiosError).toString());
    });
  }

  function getFilteredData(data: Container[]) {

    return data?.filter(
      (container: Container) => {
        return (searchColumn === 'containerID')
          ? container[searchColumn].toString().includes(searchTerm)
          : container[searchColumn].includes(searchTerm)
      }
    ) ?? [];
  }

  function onRowExpandClick(expanded: boolean, record: Container) {
    if (expanded) {
      loadRowData(record.containerID);
    }
    else {
      cancelSignal.current && cancelSignal.current.abort();
    }
  }

  function expandedRowRender(record: Container) {
    const containerId = record.containerID
    const containerKeys: ExpandedRowState = expandedRow[containerId];
    const dataSource = containerKeys?.dataSource?.map(record => ({
      ...record,
      uid: `${record.Volume}/${record.Bucket}/${record.Key}`
    })) ?? [];
    const paginationConfig: TablePaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} Keys`
    }

    return (
      <Table
        loading={containerKeys?.loading ?? true}
        dataSource={dataSource}
        columns={KEY_TABLE_COLUMNS}
        pagination={paginationConfig}
        rowKey='uid'
        locale={{ filterTitle: '' }} />
    )
  };

  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => (
      `${range[0]}-${range[1]} of ${total} Containers`
    ),
    showSizeChanger: true
  };

  return (
    <div>
      <Table
        rowKey='containerID'
        dataSource={getFilteredData(data)}
        columns={filterSelectedColumns()}
        loading={loading}
        pagination={paginationConfig}
        scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
        locale={{ filterTitle: '' }}
        expandable={{
          expandRowByClick: true,
          expandedRowRender: expandedRowRender,
          onExpand: onRowExpandClick
        }} />
    </div>
  );
}

export default ContainerTable;