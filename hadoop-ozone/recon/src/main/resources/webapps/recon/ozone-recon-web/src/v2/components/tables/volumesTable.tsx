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

import QuotaBar from '@/components/quotaBar/quotaBar';
import { byteToSize } from '@/utils/common';
import { Volume, VolumesTableProps } from '@/v2/types/volume.types';
import Table, { ColumnsType, ColumnType, TablePaginationConfig } from 'antd/es/table';
import moment from 'moment';
import React from 'react';
import { Link } from 'react-router-dom';

export const COLUMNS: ColumnsType<Volume> = [
  {
    title: 'Volume',
    dataIndex: 'volume',
    key: 'volume',
    sorter: (a: Volume, b: Volume) => a.volume.localeCompare(b.volume),
    defaultSortOrder: 'ascend' as const,
    width: '15%'
  },
  {
    title: 'Owner',
    dataIndex: 'owner',
    key: 'owner',
    sorter: (a: Volume, b: Volume) => a.owner.localeCompare(b.owner)
  },
  {
    title: 'Admin',
    dataIndex: 'admin',
    key: 'admin',
    sorter: (a: Volume, b: Volume) => a.admin.localeCompare(b.admin)
  },
  {
    title: 'Creation Time',
    dataIndex: 'creationTime',
    key: 'creationTime',
    sorter: (a: Volume, b: Volume) => a.creationTime - b.creationTime,
    render: (creationTime: number) => {
      return creationTime > 0 ? moment(creationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Modification Time',
    dataIndex: 'modificationTime',
    key: 'modificationTime',
    sorter: (a: Volume, b: Volume) => a.modificationTime - b.modificationTime,
    render: (modificationTime: number) => {
      return modificationTime > 0 ? moment(modificationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Quota (Size)',
    dataIndex: 'quotaInBytes',
    key: 'quotaInBytes',
    render: (quotaInBytes: number) => {
      return quotaInBytes && quotaInBytes !== -1 ? byteToSize(quotaInBytes, 3) : 'NA';
    }
  },
  {
    title: 'Namespace Capacity',
    key: 'namespaceCapacity',
    sorter: (a: Volume, b: Volume) => a.usedNamespace - b.usedNamespace,
    render: (text: string, record: Volume) => (
      <QuotaBar
        quota={record.quotaInNamespace}
        used={record.usedNamespace}
        quotaType='namespace'
      />
    )
  },
];

const VolumesTable: React.FC<VolumesTableProps> = ({
  loading = false,
  data,
  handleAclClick,
  selectedColumns,
  searchColumn = 'volume',
  searchTerm = ''
}) => {

  React.useEffect(() => {
    // On table mount add the actions column
    const actionsColumn: ColumnType<Volume> = {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: Volume) => {
        const searchParams = new URLSearchParams();
        searchParams.append('volume', record.volume);
  
        return (
          <>
            <Link
              key="listBuckets"
              to={`/Buckets?${searchParams.toString()}`}
              style={{
                marginRight: '16px'
              }}>
              Show buckets
            </Link>
            <a
              key='acl'
              onClick={() => handleAclClick(record)}>
              Show ACL
            </a>
          </>
        );
      }
    }

    if (COLUMNS.length > 0 && COLUMNS[COLUMNS.length - 1].key !== 'actions') {
      // Push the ACL column for initial
      COLUMNS.push(actionsColumn);
      selectedColumns.push({
        label: actionsColumn.title as string,
        value: actionsColumn.key as string
      });
    } else {
      // Replace old ACL column with new ACL column with correct reference
      // e.g. After page is reloaded / redirect from other page
      COLUMNS[COLUMNS.length - 1] = actionsColumn;
      selectedColumns[selectedColumns.length - 1] = {
        label: actionsColumn.title as string,
        value: actionsColumn.key as string
      }
    }

  }, []);

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    )
  }

  function getFilteredData(data: Volume[]) {
    return data.filter(
      (volume: Volume) => volume[searchColumn].includes(searchTerm)
    );
  }

  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} volumes`,
    showSizeChanger: true
  };

  return (
    <div>
      <Table
        dataSource={getFilteredData(data)}
        columns={filterSelectedColumns()}
        loading={loading}
        rowKey='volume'
        pagination={paginationConfig}
        scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
        locale={{ filterTitle: '' }}
      />
    </div>
  )
}

export default VolumesTable;