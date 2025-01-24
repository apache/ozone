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

import moment from 'moment';
import Table, {
  ColumnProps,
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import Tag from 'antd/es/tag';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  CloudServerOutlined,
  FileUnknownOutlined,
  HddOutlined,
  LaptopOutlined,
  SaveOutlined
} from '@ant-design/icons';

import QuotaBar from '@/components/quotaBar/quotaBar';
import { nullAwareLocaleCompare } from '@/utils/common';
import {
  Bucket,
  BucketLayout,
  BucketLayoutTypeList,
  BucketsTableProps,
  BucketStorage,
  BucketStorageTypeList
} from '@/v2/types/bucket.types';

function renderIsVersionEnabled(isVersionEnabled: boolean) {
  return isVersionEnabled
    ? <CheckCircleOutlined
      style={{ color: '#1da57a' }}
      className='icon-success' />
    : <CloseCircleOutlined className='icon-neutral' />
};

function renderStorageType(bucketStorage: BucketStorage) {
  const bucketStorageIconMap: Record<BucketStorage, React.ReactElement> = {
    RAM_DISK: <LaptopOutlined />,
    SSD: <SaveOutlined />,
    DISK: <HddOutlined />,
    ARCHIVE: <CloudServerOutlined />
  };
  const icon = bucketStorage in bucketStorageIconMap
    ? bucketStorageIconMap[bucketStorage]
    : <FileUnknownOutlined />;
  return <span>{icon} {bucketStorage}</span>;
};

function renderBucketLayout(bucketLayout: BucketLayout) {
  const bucketLayoutColorMap = {
    FILE_SYSTEM_OPTIMIZED: 'green',
    OBJECT_STORE: 'orange',
    LEGACY: 'blue'
  };
  const color = bucketLayout in bucketLayoutColorMap ?
    bucketLayoutColorMap[bucketLayout] : '';
  return <Tag color={color}>{bucketLayout}</Tag>;
};

export const COLUMNS: ColumnsType<Bucket> = [
  {
    title: 'Bucket',
    dataIndex: 'name',
    key: 'name',
    sorter: (a: Bucket, b: Bucket) => a.name.localeCompare(b.name),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Volume',
    dataIndex: 'volumeName',
    key: 'volumeName',
    sorter: (a: Bucket, b: Bucket) => a.volumeName.localeCompare(b.volumeName),
    defaultSortOrder: 'ascend' as const
  },
  {
    title: 'Owner',
    dataIndex: 'owner',
    key: 'owner',
    sorter: (a: Bucket, b: Bucket) => nullAwareLocaleCompare(a.owner, b.owner)
  },
  {
    title: 'Versioning',
    dataIndex: 'versioning',
    key: 'isVersionEnabled',
    render: (isVersionEnabled: boolean) => renderIsVersionEnabled(isVersionEnabled)
  },
  {
    title: 'Storage Type',
    dataIndex: 'storageType',
    key: 'storageType',
    filterMultiple: true,
    filters: BucketStorageTypeList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Bucket) => record.storageType === value,
    sorter: (a: Bucket, b: Bucket) => a.storageType.localeCompare(b.storageType),
    render: (storageType: BucketStorage) => renderStorageType(storageType)
  },
  {
    title: 'Bucket Layout',
    dataIndex: 'bucketLayout',
    key: 'bucketLayout',
    filterMultiple: true,
    filters: BucketLayoutTypeList.map(state => ({ text: state, value: state })),
    onFilter: (value, record: Bucket) => record.bucketLayout === value,
    sorter: (a: Bucket, b: Bucket) => a.bucketLayout.localeCompare(b.bucketLayout),
    render: (bucketLayout: BucketLayout) => renderBucketLayout(bucketLayout)
  },
  {
    title: 'Creation Time',
    dataIndex: 'creationTime',
    key: 'creationTime',
    sorter: (a: Bucket, b: Bucket) => a.creationTime - b.creationTime,
    render: (creationTime: number) => {
      return creationTime > 0 ? moment(creationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Modification Time',
    dataIndex: 'modificationTime',
    key: 'modificationTime',
    sorter: (a: Bucket, b: Bucket) => a.modificationTime - b.modificationTime,
    render: (modificationTime: number) => {
      return modificationTime > 0 ? moment(modificationTime).format('ll LTS') : 'NA';
    }
  },
  {
    title: 'Storage Capacity',
    key: 'quotaCapacityBytes',
    sorter: (a: Bucket, b: Bucket) => a.usedBytes - b.usedBytes,
    render: (text: string, record: Bucket) => (
      <QuotaBar
        quota={record.quotaInBytes}
        used={record.usedBytes}
        quotaType='size'
      />
    )
  },
  {
    title: 'Namespace Capacity',
    key: 'namespaceCapacity',
    sorter: (a: Bucket, b: Bucket) => a.usedNamespace - b.usedNamespace,
    render: (text: string, record: Bucket) => (
      <QuotaBar
        quota={record.quotaInNamespace}
        used={record.usedNamespace}
        quotaType='namespace'
      />
    )
  },
  {
    title: 'Source Volume',
    dataIndex: 'sourceVolume',
    key: 'sourceVolume',
    render: (sourceVolume: string) => {
      return sourceVolume ? sourceVolume : 'NA';
    }
  },
  {
    title: 'Source Bucket',
    dataIndex: 'sourceBucket',
    key: 'sourceBucket',
    render: (sourceBucket: string) => {
      return sourceBucket ? sourceBucket : 'NA';
    }
  }
];

const BucketsTable: React.FC<BucketsTableProps> = ({
  loading = false,
  data,
  handleAclClick,
  selectedColumns,
  searchColumn = 'name',
  searchTerm = ''
}) => {

  React.useEffect(() => {
    const aclColumn: ColumnProps<Bucket> = {
      title: 'ACLs',
      dataIndex: 'acls',
      key: 'acls',
      render: (_: any, record: Bucket) => {
        return (
          <a
            key='acl'
            onClick={() => {
              handleAclClick(record);
            }}
          >
            Show ACL
          </a>
        );
      }
    };

    if (COLUMNS.length > 0 && COLUMNS[COLUMNS.length - 1].key !== 'acls') {
      // Push the ACL column for initial load
      COLUMNS.push(aclColumn);
      selectedColumns.push({
        label: aclColumn.title as string,
        value: aclColumn.key as string
      });
    } else {
      // Replace old ACL column with new ACL column with correct reference
      // e.g. After page is reloaded / redirect from other page
      COLUMNS[COLUMNS.length - 1] = aclColumn;
      selectedColumns[selectedColumns.length - 1] = {
        label: aclColumn.title as string,
        value: aclColumn.key as string
      }
    }
  }, []);

  function filterSelectedColumns() {
    const columnKeys = selectedColumns.map((column) => column.value);
    return COLUMNS.filter(
      (column) => columnKeys.indexOf(column.key as string) >= 0
    )
  }

  function getFilteredData(data: Bucket[]) {
    return data.filter(
      (bucket: Bucket) => bucket[searchColumn].includes(searchTerm)
    );
  }

  const paginationConfig: TablePaginationConfig = {
    showTotal: (total: number, range) => `${range[0]}-${range[1]} of ${total} buckets`,
    showSizeChanger: true
  };

  return (
    <div>
      <Table
        dataSource={getFilteredData(data)}
        columns={filterSelectedColumns()}
        loading={loading}
        rowKey={(record: Bucket) => `${record.volumeName}/${record.name}`}
        pagination={paginationConfig}
        scroll={{ x: 'max-content', scrollToFirstRowOnChange: true }}
        locale={{ filterTitle: '' }}
      />
    </div>
  )
}

export default BucketsTable;