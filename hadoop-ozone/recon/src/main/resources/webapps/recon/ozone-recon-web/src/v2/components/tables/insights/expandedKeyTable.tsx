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
import filesize from 'filesize';
import Table, {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';

import { MismatchKeys } from '@/v2/types/insights.types';


const size = filesize.partial({ standard: 'iec' });

//-----Types------
type ExpandedKeyTableProps = {
  loading: boolean;
  data: MismatchKeys[];
  paginationConfig: TablePaginationConfig;
}

//-----Constants-----
const COLUMNS: ColumnsType<MismatchKeys> = [
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
    render: (date: string) => moment(date).format('lll')
  },
  {
    title: 'Date Modified',
    dataIndex: 'ModificationTime',
    key: 'ModificationTime',
    render: (date: string) => moment(date).format('lll')
  }
];

//-----Components------
const ExpandedKeyTable: React.FC<ExpandedKeyTableProps> = ({
  loading,
  data,
  paginationConfig
}) => {
  return (
    <Table
      loading={loading}
      dataSource={data}
      columns={COLUMNS}
      pagination={paginationConfig}
      rowKey='uid'
      locale={{ filterTitle: '' }} />
  )
}

export default ExpandedKeyTable;