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
import Table, {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';

import { byteToSize } from '@/utils/common';
import { getFormattedTime } from '@/v2/utils/momentUtils';

import { DeletePendingKey } from '@/v2/types/insights.types';

//--------Types--------
type ExpandedPendingKeysTableProps = {
  data: DeletePendingKey[];
  paginationConfig: TablePaginationConfig;
}

//--------Constants--------
const COLUMNS: ColumnsType<DeletePendingKey> = [{
  title: 'Data Size',
  dataIndex: 'dataSize',
  key: 'dataSize',
  render: (dataSize: any) => dataSize = dataSize > 0 ? byteToSize(dataSize, 1) : dataSize
},
{
  title: 'Replicated Data Size',
  dataIndex: 'replicatedSize',
  key: 'replicatedSize',
  render: (replicatedSize: any) => replicatedSize = replicatedSize > 0 ? byteToSize(replicatedSize, 1) : replicatedSize
},
{
  title: 'Creation Time',
  dataIndex: 'creationTime',
  key: 'creationTime',
  render: (creationTime: number) => {
    return getFormattedTime(creationTime, 'll LTS');
  }
},
{
  title: 'Modification Time',
  dataIndex: 'modificationTime',
  key: 'modificationTime',
  render: (modificationTime: number) => {
    return getFormattedTime(modificationTime, 'll LTS');
  }
}]

//--------Component--------
const ExpandedPendingKeysTable: React.FC<ExpandedPendingKeysTableProps> = ({
  data,
  paginationConfig
}) => {
  return (
    <Table
      dataSource={data}
      columns={COLUMNS}
      pagination={paginationConfig}
      rowKey='dataSize'
      locale={{ filterTitle: '' }} />
  )
}

export default ExpandedPendingKeysTable;