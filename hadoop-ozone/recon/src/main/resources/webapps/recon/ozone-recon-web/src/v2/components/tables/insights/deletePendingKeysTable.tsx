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
import Table, {
  ColumnsType,
  TablePaginationConfig
} from 'antd/es/table';
import { ValueType } from 'react-select';

import SingleSelect, { Option } from '@/v2/components/select/singleSelect';
import ExpandedPendingKeysTable from '@/v2/components/tables/insights/expandedPendingKeysTable';
import { AxiosGetHelper } from '@/utils/axiosRequestHelper';
import { byteToSize, showDataFetchError } from '@/utils/common';
import { LIMIT_OPTIONS } from '@/v2/constants/limit.constants';

import {
  DeletePendingKey,
  DeletePendingKeysResponse
} from '@/v2/types/insights.types';

//-----Types------
type DeletePendingKeysTableProps = {
  paginationConfig: TablePaginationConfig
  limit: Option;
  handleLimitChange: (arg0: ValueType<Option, false>) => void;
}

type DeletePendingKeysColumns = {
  fileName: string;
  keyName: string;
  dataSize: number;
  keyCount: number;
}

type ExpandedDeletePendingKeys = {
  omKeyInfoList: DeletePendingKey[]
}

//------Constants------
const COLUMNS: ColumnsType<DeletePendingKeysColumns> = [
  {
    title: 'Key Name',
    dataIndex: 'fileName',
    key: 'fileName'
  },
  {
    title: 'Path',
    dataIndex: 'keyName',
    key: 'keyName',
  },
  {
    title: 'Total Data Size',
    dataIndex: 'dataSize',
    key: 'dataSize',
    render: (dataSize: number) => byteToSize(dataSize, 1)
  },
  {
    title: 'Total Key Count',
    dataIndex: 'keyCount',
    key: 'keyCount',
  }
];

let expandedDeletePendingKeys: ExpandedDeletePendingKeys[] = [];

//-----Components------
const DeletePendingKeysTable: React.FC<DeletePendingKeysTableProps> = ({
  paginationConfig,
  limit,
  handleLimitChange
}) => {
  const [loading, setLoading] = React.useState<boolean>(false);
  const [data, setData] = React.useState<DeletePendingKeysColumns[]>();

  const cancelSignal = React.useRef<AbortController>();

  function expandedRowRender(record: DeletePendingKeysColumns) {
    console.log(expandedDeletePendingKeys);
    const filteredData = expandedDeletePendingKeys?.flatMap((info) => (
      info.omKeyInfoList?.filter((key) => key.keyName === record.keyName)
    ));
    return (
      <ExpandedPendingKeysTable
        data={filteredData}
        paginationConfig={paginationConfig} />
    )
  }

  function fetchDeletePendingKeys() {
    setLoading(true);
    const { request, controller } = AxiosGetHelper(
      `/api/v1/keys/deletePending?limit=${limit.value}`,
      cancelSignal.current
    );
    cancelSignal.current = controller;

    request.then(response => {
      const deletePendingKeys: DeletePendingKeysResponse = response?.data;
      let deletedKeyData = [];
      // Sum up the data size and organize related key information
      deletedKeyData = deletePendingKeys?.deletedKeyInfo.flatMap((keyInfo) => {
        expandedDeletePendingKeys.push(keyInfo);
        let count = 0;
        let item: DeletePendingKey = keyInfo.omKeyInfoList?.reduce((obj, curr) => {
          count += 1;
          return { ...curr, dataSize: obj.dataSize + curr.dataSize };
        }, { ...keyInfo.omKeyInfoList[0], dataSize: 0 });

        return {
          dataSize: item.dataSize,
          fileName: item.fileName,
          keyName: item.keyName,
          path: item.path,
          keyCount: count
        }
      });
      setData(deletedKeyData);
      setLoading(false);
    }).catch(error => {
      setLoading(false);
      showDataFetchError((error as AxiosError).toString());
    })
  }

  React.useEffect(() => {
    fetchDeletePendingKeys();

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
          expandedRowRender: expandedRowRender
        }}
        dataSource={data}
        columns={COLUMNS}
        loading={loading}
        pagination={paginationConfig}
        rowKey='keyName'
        locale={{ filterTitle: '' }}
        scroll={{ x: 'max-content' }} />
    </>
  )
}

export default DeletePendingKeysTable;