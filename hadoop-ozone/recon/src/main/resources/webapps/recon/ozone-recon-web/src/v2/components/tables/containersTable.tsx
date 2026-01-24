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
import filesize from 'filesize';

import { Popover, Table } from 'antd';
import {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import { CheckCircleOutlined, NodeIndexOutlined } from '@ant-design/icons';

import {getFormattedTime} from '@/v2/utils/momentUtils';
import {showDataFetchError} from '@/utils/common';
import {fetchData} from '@/v2/hooks/useAPIData.hook';
import {
  Container,
  ContainerKeysResponse,
  ContainerReplica,
  ContainerTableProps,
  ExpandedRowState,
  KeyResponse
} from '@/v2/types/container.types';

const size = filesize.partial({ standard: 'iec' });

const getDatanodeWith = (idx: number, host: string) => {
  return (
    <div key={idx} className='datanode-container-v2-tr'>
      <NodeIndexOutlined /> {host}
    </div>
  );
}

const getDataNodeWithChecksum = (idx: number, host: string, checksum: string) => {
  return (
    <div key={idx} className='datanode-container-v2-tr'>
      <span className="datanode-container-v2-td"><NodeIndexOutlined /> {host}</span>
      <span className="datanode-container-v2-td">/ {checksum}</span>
    </div>
  )
}

export const COLUMNS: ColumnsType<Container> = [
  {
    title: 'Container ID',
    dataIndex: 'containerID',
    key: 'containerID',
    sorter: (a: Container, b: Container) => a.containerID - b.containerID
  },
  {
    title: 'No of Blocks',
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
    render: (replicas: ContainerReplica[]) => {
      const renderDatanodes = (replicas: ContainerReplica[]) => {
        return (
          <div className="datanode-container-v2-table">
            {
              replicas?.map((replica: any, idx: number) => (
                (replica.dataChecksum)
                  ? getDataNodeWithChecksum(idx, replica.datanodeHost, replica.dataChecksum)
                  : getDatanodeWith(idx, replica.datanodeHost)
              ))}
          </div>
        )
      }

      return (
        <Popover
          content={renderDatanodes(replicas)}
          title={
            replicas.some((replica => replica.hasOwnProperty('dataChecksum')))
            ? 'Datanodes / Checksum'
            : 'Datanodes'
          }
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
  },
  {
    title: 'Path',
    dataIndex: 'CompletePath',
    key: 'path'
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


  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    );
  }

  async function loadRowData(containerID: number) {
    try {
      const containerKeysResponse = await fetchData<ContainerKeysResponse>(
        `/api/v1/containers/${containerID}/keys`
      );
      
      expandedRowSetter({
        ...expandedRow,
        [containerID]: {
          ...expandedRow[containerID],
          loading: false,
          dataSource: containerKeysResponse.keys,
          totalCount: containerKeysResponse.totalCount
        }
      });
    } catch (error) {
      expandedRowSetter({
        ...expandedRow,
        [containerID]: {
          ...expandedRow[containerID],
          loading: false
        }
      });
      showDataFetchError(error);
    }
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
  }

  function expandedRowRender(record: Container) {
    const containerId = record.containerID
    const containerKeys: ExpandedRowState = expandedRow[containerId];
    const dataSource = containerKeys?.dataSource ?? [];
    const paginationConfig: TablePaginationConfig = {
      showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} Keys`
    }

    return (
      <Table
        loading={containerKeys?.loading ?? true}
        dataSource={dataSource}
        columns={KEY_TABLE_COLUMNS}
        pagination={paginationConfig}
        rowKey={(record: KeyResponse) => record.CompletePath}
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
