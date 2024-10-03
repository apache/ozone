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
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';

import { Log, LogTableProps } from '@/v2/types/logs.types';

export const COLUMNS: ColumnsType<Log> = [
  {
    title: 'Timestamp',
    dataIndex: 'timestamp',
    key: 'timestamp',
  },
  {
    title: 'Log Level',
    dataIndex: 'level',
    key: 'level'
  },
  {
    title: 'Source',
    dataIndex: 'source',
    key: 'source'
  },
  {
    title: 'Message',
    dataIndex: 'message',
    key: 'message',
  }
];

const LogsTable: React.FC<LogTableProps> = ({
  loading = false,
  data
}) => {

  function parseLogData(data: Log[]) {
    // We need to parse the date from timestamp to
    // human readable time
    return data.map((log) => ({
      timestap: moment(log.timestamp).format('ll LTS'),
      ...log
    }));
  }

  const parsedData = React.useMemo(
    () => parseLogData(data),
    [data.length]
  );


  return (
    <div>
      <Table
        dataSource={parsedData}
        columns={COLUMNS}
        loading={loading}
        rowKey='message'
        scroll={{ x: 'max-content', y: 400, scrollToFirstRowOnChange: false }}
        locale={{ filterTitle: '' }}
      />
    </div>
  )
}

export default LogsTable;